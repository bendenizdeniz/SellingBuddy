using EventBus.Base;
using EventBus.Base.Events;
using Newtonsoft.Json;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace EventBus.RabbitMQ
{
    public class EventBusRabbitMQ : BaseEventBus
    {
        RabbitMQPersistentConnection persistentConnection;
        private readonly IConnectionFactory connectionFactory;
        private readonly IModel consumerChannel;

        public EventBusRabbitMQ(IServiceProvider serviceProvider, EventBusConfig eventBusConfig) : base(serviceProvider, eventBusConfig)
        {
            if (eventBusConfig.Connection is not null)
            {
                var connectionJson = JsonConvert.SerializeObject(eventBusConfig.Connection, new JsonSerializerSettings()
                {
                    //Self referencing loop detected for property.
                    ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                });

                connectionFactory = JsonConvert.DeserializeObject<ConnectionFactory>(connectionJson);
            }
            else
            {
                connectionFactory = new ConnectionFactory();
            }

            persistentConnection = new RabbitMQPersistentConnection(connectionFactory, eventBusConfig.ConnectionRetryCount);

            consumerChannel = CreateConsumerChannel();

            SubscriptionManager.OnEventRemoved += SubscriptionManager_OnEventRemoved;
        }

        private void SubscriptionManager_OnEventRemoved(object? sender, string eventName)
        {
            eventName = ProcessEventName(eventName);

            CheckIsConnected();

            consumerChannel.QueueUnbind(queue: eventName, exchange: EventBusConfig.DefaultTopicName, routingKey: eventName);

            if (SubscriptionManager.IsEmpty)
            {
                consumerChannel.Close();
            }
        }

        public override void Publish(IntegrationEvent @event)
        {
            var policy = Policy.Handle<BrokerUnreachableException>()
                .Or<SocketException>()
                .WaitAndRetry(EventBusConfig.ConnectionRetryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) => { });

            var eventName = @event.GetType().Name;
            eventName = ProcessEventName(eventName);

            consumerChannel.ExchangeDeclare(exchange: EventBusConfig.DefaultTopicName, type: "direct");

            var message = JsonConvert.SerializeObject(@event);
            var body = Encoding.UTF8.GetBytes(message);

            policy.Execute(() =>
            {
                var properties = consumerChannel.CreateBasicProperties();
                properties.DeliveryMode = 2; //persistent

                DeclareQueue(eventName);

                consumerChannel.BasicPublish(
                    exchange: EventBusConfig.DefaultTopicName,
                    routingKey: eventName,
                    mandatory: true,
                    basicProperties: properties,
                    body: body
                    );
            });
        }

        public override void Subscribe<T, TH>()
        {
            var eventName = typeof(T).Name;
            eventName = ProcessEventName(eventName);

            if (!SubscriptionManager.HasSubscriptionsForEvent(eventName))
            {
                CheckIsConnected();

                DeclareQueue(eventName);

                consumerChannel.QueueBind(queue: GetSubName(eventName), exchange: EventBusConfig.DefaultTopicName, routingKey: eventName);
            }

            SubscriptionManager.AddSubscription<T, TH>();

            StartBasicConsume(eventName);
        }

        public override void UnSubscribe<T, TH>()
        {
            SubscriptionManager.RemoveSubsciption<T, TH>();
        }

        private IModel CreateConsumerChannel()
        {
            CheckIsConnected();

            var channel = persistentConnection.CreateModel();

            channel.ExchangeDeclare(EventBusConfig.DefaultTopicName, "direct");

            return channel;
        }

        private void StartBasicConsume(string eventName)
        {
            if (consumerChannel is not null)
            {
                var consumer = new EventingBasicConsumer(consumerChannel);

                consumer.Received += Consumer_Received;

                consumerChannel.BasicConsume(queue: GetSubName(eventName), autoAck: false, consumer: consumer);
            }
        }

        private void Consumer_Received(object? sender, BasicDeliverEventArgs e)
        {
            var eventName = e.RoutingKey;
            eventName = ProcessEventName(eventName);
            var message = Encoding.UTF8.GetString(e.Body.Span);

            try
            {
                ProcessEvent(eventName, message).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error occured when message received! {0}", ex);
            }

            consumerChannel.BasicAck(deliveryTag: e.DeliveryTag, multiple: false);
        }

        private void CheckIsConnected()
        {
            if (!persistentConnection.isConnected)
                persistentConnection.TryConnect();
        }

        private void DeclareQueue(string eventName)
        {
            consumerChannel.QueueDeclare(queue: GetSubName(eventName), durable: true, exclusive: false, autoDelete: false, arguments: null);
        }
    }
}
