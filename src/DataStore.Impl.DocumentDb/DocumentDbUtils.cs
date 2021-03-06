﻿namespace DataStore.Impl.DocumentDb
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using DataStore.Models;
    using Microsoft.Azure.Documents;

    public static class DocumentDbUtils
    {
        /// <summary>
        ///     Method that will detect "go slower" messages from DocDb and wait the correct time
        ///     It does not currently retry on network failures, though this really should be added
        /// </summary>
        /// <typeparam name="TV">The return value of your lambda</typeparam>
        /// <param name="function">The lambda you want to run</param>
        /// <returns>The output of your operation</returns>
        public static async Task<TV> ExecuteWithRetries<TV>(Func<Task<TV>> function)
        {
            DocumentClientException retryableException = null;
            var retryCount = 0;
            while (retryCount < 10)
            {
                TimeSpan? timeToSleepBeforeRetry;

                try
                {
                    return await function().ConfigureAwait(false);
                }
                catch (DocumentClientException clientException)
                {
                    if (IsRetryableError(clientException))
                    {
                        timeToSleepBeforeRetry = clientException.RetryAfter;
                        retryableException = clientException;
                    }
                    else
                    {
                        throw;
                    }
                }
                catch (AggregateException aggregateException)
                {
                    var exception = aggregateException.InnerException as DocumentClientException;

                    if (exception != null)
                    {
                        var documentClientException = exception;
                        if (IsRetryableError(documentClientException))
                        {
                            timeToSleepBeforeRetry = documentClientException.RetryAfter;
                            retryableException = documentClientException;
                        }
                        else
                        {
                            throw;
                        }
                    }
                    else
                    {
                        throw;
                    }
                }

                retryCount++;
                await Task.Delay(timeToSleepBeforeRetry.Value).ConfigureAwait(false);
            }

            throw new DatabaseException("Document db exception, retries exceeded", retryableException);
        }

        /// <summary>
        ///     Method that will detect "go slower" messages from DocDb and wait the correct time
        ///     It does not currently retry on network failures, though this really should be added
        /// </summary>
        /// <typeparam name="TV">The return value of your lambda</typeparam>
        /// <param name="function">The lambda you want to run</param>
        /// <returns>The output of your operation</returns>
        public static TV ExecuteWithRetries<TV>(Func<TV> function)
        {
            DocumentClientException retryableException = null;
            var retryCount = 0;
            while (retryCount < 10)
            {
                TimeSpan? timeToSleepBeforeRetry;

                try
                {
                    return function();
                }
                catch (DocumentClientException clientException)
                {
                    if (IsRetryableError(clientException))
                    {
                        timeToSleepBeforeRetry = clientException.RetryAfter;
                        retryableException = clientException;
                    }
                    else
                    {
                        throw;
                    }
                }
                catch (AggregateException aggregateException)
                {
                    var exception = aggregateException.InnerException as DocumentClientException;

                    if (exception != null)
                    {
                        var documentClientException = exception;
                        if (IsRetryableError(documentClientException))
                        {
                            timeToSleepBeforeRetry = documentClientException.RetryAfter;
                            retryableException = documentClientException;
                        }
                        else
                        {
                            throw;
                        }
                    }
                    else
                    {
                        throw;
                    }
                }

                retryCount++;
                Thread.Sleep(timeToSleepBeforeRetry.Value);
            }

            throw new DatabaseException("Document db exception, retries exceeded", retryableException);
        }

        private static bool IsRetryableError(DocumentClientException de)
        {
            var requestRateExceeded = 429;
            var transientErrorSafeToRetry = 449;
            return de.StatusCode != null && ((int)de.StatusCode == requestRateExceeded || (int)de.StatusCode == transientErrorSafeToRetry);
        }
    }
}