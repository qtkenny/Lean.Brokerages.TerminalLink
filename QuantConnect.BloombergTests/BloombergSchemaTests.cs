using System;
using System.IO;
using System.Text;
using Bloomberglp.Blpapi;
using NUnit.Framework;

namespace QuantConnect.BloombergTests
{
#if LIVE_API
    [TestFixture(Ignore = true, IgnoreReason = "Don't execute by default")]
    public class BloombergSchemaTests
    {
        private readonly SessionOptions _sessionOptions = new SessionOptions {AutoRestartOnDisconnection = false, NumStartAttempts = 1};
        private Session _session;
        private DirectoryInfo _baseOutput;

        [Test]
        [TestCase("EMSX", "//blp/emapisvc_beta")]
        [TestCase("MarketData", "//blp/mktdata")]
        [TestCase("RefData", "//blp/refdata")]
        public void Create_Documentation(string serviceName, string serviceUri)
        {
            var output = _baseOutput.CreateSubdirectory(serviceName);
            if (!_session.OpenService(serviceUri))
            {
                throw new Exception("Unable to open the EMSX service");
            }

            var service = _session.GetService(serviceUri);
            foreach (var operation in service.Operations)
            {
                WriteDocumentation(output, operation, x => x.Name.ToString());
            }

            foreach (var eventDef in service.EventDefinitions)
            {
                WriteDocumentation(output, eventDef, x => x.Name.ToString());
            }
        }

        private static void WriteDocumentation<T>(FileSystemInfo output, T bbgObject, Func<T, string> nameFunc)
        {
            var name = $"{typeof(T).Name}-{nameFunc(bbgObject)}.txt";
            var fileName = Path.Combine(output.FullName, MakeSafeFileName(name));
            if (File.Exists(fileName))
            {
                throw new IOException("File already exists, where the name should have been unique: " + fileName);
            }

            File.WriteAllText(fileName, bbgObject.ToString(), Encoding.UTF8);
            Console.Out.WriteLine("Created: " + fileName);
        }

        private static string MakeSafeFileName(string input)
        {
            var result = new StringBuilder(input);
            foreach (var c in Path.GetInvalidFileNameChars())
            {
                result.Replace(c, '_');
            }

            return result.ToString();
        }

        [TestFixtureSetUp]
        public void SetupBloomberg()
        {
            var currentDirectory = new DirectoryInfo(TestContext.CurrentContext.WorkDirectory);
            if (!currentDirectory.Exists)
            {
                throw new DirectoryNotFoundException("Directory doesn't exist: " + currentDirectory);
            }

            const string outputDirectoryName = "Documentation";
            foreach (var child in currentDirectory.GetDirectories(outputDirectoryName, SearchOption.TopDirectoryOnly))
            {
                child.Delete(true);
            }

            _baseOutput = currentDirectory.CreateSubdirectory(outputDirectoryName);
            _session = new Session(_sessionOptions);
            if (!_session.Start())
            {
                throw new Exception("Unable to open Bloomberg");
            }
        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            _session?.Stop(AbstractSession.StopOption.SYNC);
        }
    }
#endif
}