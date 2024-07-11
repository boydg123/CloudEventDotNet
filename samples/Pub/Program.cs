// See https://aka.ms/new-console-template for more information
using SamplesCommon;

Console.WriteLine("Hello, World!");

Tester tester = new PingTester();

await tester.RunAsync(new string[] { "", "pub", "5", "10000" });
