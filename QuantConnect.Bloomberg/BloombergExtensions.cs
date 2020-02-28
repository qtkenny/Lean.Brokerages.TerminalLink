using System;
using System.Collections.Generic;
using Bloomberglp.Blpapi;
using QuantConnect.Logging;

namespace QuantConnect.Bloomberg
{
    internal static class BloombergExtensions
    {
        /// <summary>
        /// Sends a request to Bloomberg and waits for the response.
        /// </summary>
        /// <param name="session">Active Bloomberg session</param>
        /// <param name="request">Request to send</param>
        /// <returns></returns>
        public static IEnumerable<Message> SendRequestSynchronous(this Session session, Request request)
        {
            var queue = new EventQueue();
            try
            {
                var correlationId = BloombergBrokerage.GetNewCorrelationId();
                Log.Trace($"BloombergExtensions.SendRequestSynchronous(): Sending request '{request.Operation.Name}' ({correlationId}): {request}");
                session.SendRequest(request, queue, correlationId);
                Event evt;
                do
                {
                    evt = queue.NextEvent();
                    foreach (var message in evt.GetMessages())
                    {
                        yield return message;
                    }
                }
                while (evt.Type != Event.EventType.RESPONSE);
            }
            finally
            {
                queue.Purge();
            }
        }

        /// <summary>
        /// Extracts Lean's order id out of a message, if one exists.
        /// </summary>
        /// <param name="message"></param>
        /// <returns>The sequence id or -1</returns>
        internal static int GetSequence(this Message message)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            return message.HasElement(BloombergNames.EMSXSequence) ? message.GetElementAsInt32(BloombergNames.EMSXSequence) : -1;
        }
    }
}