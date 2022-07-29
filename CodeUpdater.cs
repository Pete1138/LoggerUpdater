using System.IO;
using System.Text.RegularExpressions;

namespace LoggerUpdater
{
    public enum CodeMarker
    {
        UsingStatements = 0,
        NamespaceStart,
        ClassStart,
        ConstructorStart,
        ConstructorEnd,
        ClassEnd,
        NamespaceEnd
    };

    public class CodeUpdater
    {
        private CodeMarker _currentCodeMarker;
        private string _className;
        private int _lineNumber;

        public void UpdateLogger(string path)
        {
            _currentCodeMarker = CodeMarker.UsingStatements;
            _lineNumber = 1;

            if (!System.IO.File.Exists(path)) throw new FileNotFoundException("The file doesn't exist!", path);
            var lines = File.ReadAllLines(path)?.ToList();
            if (!lines?.Any() ?? false) throw new Exception($"No lines in the file {path}");

            var newLines = ProcessAllLines(lines);

            if (lines != newLines)
            {
                Console.WriteLine($"Backing up {path} to {path}.orig");
                File.Copy(path, path + ".orig");
                Console.WriteLine($"Writing updated file to {path}.new");
                File.WriteAllLines(path + ".new", newLines.ToArray());
            }
        }

        public List<string> ProcessAllLines(List<string> lines)
        {
            var newLines = new List<string>();

            bool done = false;
            foreach (var line in lines)
            {
                if (done) return lines;
                try
                {
                    switch (_currentCodeMarker)
                    {
                        case CodeMarker.UsingStatements: newLines.Add(ProcessUsingStatements(line)); break;
                        case CodeMarker.NamespaceStart:
                            done = !ProcessNamespaceStart(line);
                            newLines.Add(line);
                            break;

                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Oops! Wasn't expecting that: {Exception}", ex.ToString());
                }


                _lineNumber++;
            }

            return newLines;
        }

        public string ProcessILoggerType(string line)
        {
            if (line.Contains("ILogger<"))
            {
                var updatedILoggerType = Regex.Replace(line, @"ILogger<.*>", "ILogger");
                return updatedILoggerType;
            }

            return line;
        }

        public string ProcessUsingStatements(string line)
        {
            if (line == "using Microsoft.Extensions.Logging;")
            {
                return "using Serilog;";
            }
            else if (line.StartsWith("namespace "))
            {
                _currentCodeMarker = CodeMarker.NamespaceStart;
            }

            return line;
        }

        public bool ProcessNamespaceStart(string line)
        {
            if (line.Trim() == ("{")) return true;
            if (line.Trim() == String.Empty) return true;
            if (line.Trim().StartsWith("//")) return true;
            var regex = new Regex(@"(?<modifier>(public|protected|internal|sealed|private)) (?<type>(class|interface|struct|enum|record)) (?<className>\w+)");
            var match = regex.Match(line);
            if (match.Success)
            {
                _className = match.Groups["className"]?.Value;
                var type = match.Groups["type"]?.Value;
                Console.WriteLine($"Found declaration of {type} '{_className}'");
                if (type != "class")
                {
                    Console.WriteLine($"{_className} is not a class. Will not process");
                    return false;
                }
                if (_className == string.Empty) throw new Exception($"Unable to find class name in line '{line}' ({_lineNumber}");
            }

            return true;
        }
    }
}
