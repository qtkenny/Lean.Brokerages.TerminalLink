/*
* QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
* Lean Algorithmic Trading Engine v2.2 Copyright 2015 QuantConnect Corporation.
*/

using System;
using System.Collections.Generic;
using Bloomberglp.Blpapi;
using QuantConnect.Logging;

namespace QuantConnect.TerminalLink
{
    internal static class TerminalLinkExtensions
    {
        /// <summary>
        /// Sends a request to TerminalLink and waits for the response.
        /// </summary>
        /// <param name="session">Active TerminalLink session</param>
        /// <param name="request">Request to send</param>
        /// <returns></returns>
        public static IEnumerable<Message> SendRequestSynchronous(this Session session, Request request)
        {
            var queue = new EventQueue();
            try
            {
                var correlationId = TerminalLinkBrokerage.GetNewCorrelationId();
                Log.Trace($"TerminalLinkExtensions.SendRequestSynchronous(): Sending request '{request.Operation.Name}' ({correlationId}): {request}");
                session.SendRequest(request, queue, correlationId);

                Event evt;
                do
                {
                    evt = queue.NextEvent();

                    // queue.NextEvent() can return the following event types:
                    // - Event.EventType.PARTIAL_RESPONSE - partial response
                    // - Event.EventType.RESPONSE - final response
                    // - Event.EventType.REQUEST_STATUS - error
                    //   - event name: RequestFailure
                    //   - source: RequestManager
                    //   - error code: -1
                    //   - category - description:
                    //     - UNCLASSIFIED - Unknown request error
                    //     - IO_ERROR - Request failed on lost connection
                    //     - TIMEOUT - Request timed out at backend
                    //     - CANCELED - Request cancelled due to authorization failure

                    foreach (var message in evt.GetMessages())
                    {
                        yield return message;
                    }
                }
                while (evt.Type != Event.EventType.RESPONSE && evt.Type != Event.EventType.REQUEST_STATUS);
            }
            finally
            {
                queue.Purge();
            }
        }

        /// <summary>
        /// Returns whether the message contains a request failure element
        /// </summary>
        /// <param name="msg">The input message</param>
        /// <returns>true if the message contains a request failure element</returns>
        internal static bool IsFailed(this Message msg)
        {
            return msg.HasElement(TerminalLinkNames.RequestFailure);
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

            return message.HasElement(TerminalLinkNames.EMSXSequence) ? message.GetElementAsInt32(TerminalLinkNames.EMSXSequence) : -1;
        }
    }
}