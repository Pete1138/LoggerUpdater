using LoggerUpdater;

var path = @"C:\Code\Other\LoggerUpdater\BookingManagementService.cs";
var updater = new CodeUpdater();
updater.UpdateLogger(path);

Console.ReadLine();
// exit if not a class
// no logger
   // do nothing
// ILogger
// remove using statement
// remove constructor + private member variable (store name)
// update all logger statements (debug,trace,warning,error,information)
// serilog logger (already fixed)
