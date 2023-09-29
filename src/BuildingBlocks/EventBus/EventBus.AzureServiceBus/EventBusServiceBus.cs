using EventBus.Base;
using EventBus.Base.Events;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventBus.AzureServiceBus
{
    public class EventBusServiceBus : BaseEventBus
    {
        private ITopicClient topicClient;
        private ManagementClient managementClient;

        public EventBusServiceBus(IServiceProvider serviceProvider, EventBusConfig eventBusConfig) : base(serviceProvider, eventBusConfig)
        {
            managementClient = new ManagementClient(eventBusConfig.EventBusConnectionString);

            topicClient = CreateTopicClient();
        }

        private ITopicClient CreateTopicClient()
        {
            if (topicClient != null || topicClient.IsClosedOrClosing)
            {
                topicClient = new TopicClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName);
            }

            //check the topic is already exists
            if (!managementClient.TopicExistsAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult())
            {
                managementClient.CreateTopicAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult();
            }

            return topicClient;
        }

        public override void Publish(IntegrationEvent @event)
        {
            var eventName = @event.GetType().Name;
            var currentLabel = ProcessEventName(eventName);
            var eventStr = JsonConvert.SerializeObject(@event);
            var body = Encoding.UTF8.GetBytes(eventStr);

            Message message = new Message()
            {
                MessageId = Guid.NewGuid().ToString(),
                Body = body,
                Label = currentLabel,


            };

            topicClient.SendAsync(message).GetAwaiter().GetResult();
        }

        public override void Subscribe<T, TH>()
        {
            var eventName = typeof(T).Name;
            eventName = ProcessEventName(eventName);

            if (!SubscriptionManager.HasSubscriptionsForEvent(eventName))
            {
                var subscriptionClient = CreateSubscriptionClientIfNotExists(eventName);

                RegisterSubscriptionClientMessageHandler(subscriptionClient);
            }

            Console.WriteLine("Subscribing to event {EventName} with {EventHandler}", eventName, typeof(TH).Name);

            SubscriptionManager.AddSubscription<T, TH>();
        }

        public override void UnSubscribe<T, TH>()
        {
            var eventName = typeof(T).Name;

            try
            {
                //Subscription will be there but we dont subscribe by removing the rule that we subscribed.
                var subscriptionClient = CreateSubscriptionClientObject(eventName);

                subscriptionClient.RemoveRuleAsync(eventName).GetAwaiter().GetResult();
            }
            catch (MessagingEntityNotFoundException ex)
            {
                Console.WriteLine("The messaging entity {eventName} could not be found. {exception}", eventName, ex);
            }
        }

        private void RegisterSubscriptionClientMessageHandler(ISubscriptionClient subscriptionClient)
        {
            subscriptionClient.RegisterMessageHandler(async (message, handler) =>
            {
                var eventName = $"{message.Label}";

                var messageData = Encoding.UTF8.GetString(message.Body);

                //Complete the message so that it is not received again.
                if (await ProcessEvent(ProcessEventName(eventName), messageData))
                {
                    await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
                }

            }, new MessageHandlerOptions(ExceptionReceiverHandler) { MaxConcurrentCalls = 10, AutoComplete = false });
        }

        private Task ExceptionReceiverHandler(ExceptionReceivedEventArgs ex)
        {
            var context = ex.ExceptionReceivedContext;

            Console.WriteLine("ERROR handling message: {ExceptionMessage} - Context: {@ExceptionContext}", ex.Exception.Message, context);

            return Task.CompletedTask;
        }

        private ISubscriptionClient CreateSubscriptionClientIfNotExists(string eventName)
        {
            var subscriptionClientObj = CreateSubscriptionClientObject(eventName);

            bool exists = managementClient.SubscriptionExistsAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();

            if (subscriptionClientObj is not null && !exists)
            {
                managementClient.CreateSubscriptionAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();

                RemoveDefaultRuleFromSubscriptionClient(subscriptionClientObj);
            }

            CreateRuleIfNotExists(ProcessEventName(eventName), subscriptionClientObj);

            return subscriptionClientObj;
        }

        private SubscriptionClient CreateSubscriptionClientObject(string eventName)
        {
            return new SubscriptionClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName, GetSubName(eventName));
        }

        private void CreateRuleIfNotExists(string eventName, ISubscriptionClient subscriptionClient)
        {
            bool ruleExists;

            try
            {
                var rule = managementClient.GetRuleAsync(EventBusConfig.DefaultTopicName, eventName, eventName).GetAwaiter().GetResult();

                ruleExists = rule != null;
            }
            catch (MessagingEntityNotFoundException)
            {
                ruleExists = false;
            }

            if (!ruleExists)
            {
                RuleDescription ruleDescription = new RuleDescription()
                {
                    Filter = new CorrelationFilter() { Label = eventName },
                    Name = eventName
                };

                managementClient.CreateRuleAsync(EventBusConfig.DefaultTopicName, eventName, ruleDescription).GetAwaiter().GetResult();
            }
        }

        private void RemoveDefaultRuleFromSubscriptionClient(SubscriptionClient subscriptionClient)
        {
            try
            {
                subscriptionClient.RemoveRuleAsync(RuleDescription.DefaultRuleName).GetAwaiter().GetResult();
            }
            catch (MessagingEntityNotFoundException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public override void Dispose()
        {
            base.Dispose();

            topicClient.CloseAsync().GetAwaiter().GetResult();
            managementClient.CloseAsync().GetAwaiter().GetResult();

            topicClient = null;
            managementClient = null;
        }

    }
}
