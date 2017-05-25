using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Odbc;
using System.Configuration;

namespace SQL_Export
{
    class SQLtoCSV
    {
        public static void SQLToCSV(string connectionString, string QueryString, string filepath, string Filename)
        {

            OdbcCommand command = new OdbcCommand(connectionString);
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                command.Connection = connection;
                connection.Open();
                command.CommandText = QueryString;
                // Execute the DataReader and access the data.
                OdbcDataReader reader = command.ExecuteReader();
                using (System.IO.StreamWriter fs = new System.IO.StreamWriter(filepath + Filename))
                {
                    // Loop through the fields and add headers
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        string name = reader.GetName(i);
                        //Avoid delimiter issues by encasing fields with commas in backslashes.
                        if (name.Contains(","))
                            name = "\"" + name + "\"";

                        fs.Write(name + ",");
                    }
                    fs.WriteLine();

                    // Loop through the rows and output the data
                    while (reader.Read())
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            string value = reader[i].ToString();
                            //Avoid delimiter issues by encasing fields with commas in backslashes.
                            if (value.Contains(","))
                                value = "\"" + value + "\"";

                            fs.Write(value + ",");
                        }
                        fs.WriteLine();
                    }

                    fs.Close();
                }
            }

        }
    }
    
    class Program
    {

 
        static void Main(string[] args)
        {
            //Set variables
            string connectionString = ConfigurationManager.ConnectionStrings["OdbcConnection"].ToString();
            string filepath_root = ConfigurationManager.ConnectionStrings["rootPath"].ToString();
            string QueryString = @"SELECT [OrganisationID]
                                    FROM[CMA_Consistency].[CMA].[Participants]";
            string filepath = "";
            string Filename = "";
            OdbcCommand command = new OdbcCommand();
            //Get the LPs from the database
            List<string> LP_list = new List<string>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                command.Connection = connection;
                connection.Open();
                command.CommandText = QueryString;
                // Execute the DataReader and access the data.
                OdbcDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    LP_list.Add(reader[0].ToString());
                }
                // Call Close when done reading.
                reader.Close();
            }
            //Get the table names from SQL where the results are
            QueryString = @"SELECT t.name
                                   FROM sys.tables AS t
                                   INNER JOIN sys.schemas AS s
                                   ON t.[schema_id] = s.[schema_id]
                                   WHERE s.name = N'Comp';";
            List<string> Table_list = new List<string>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                command.Connection = connection;
                connection.Open();
                command.CommandText = QueryString;
                // Execute the DataReader and access the data.
                OdbcDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    Table_list.Add(reader[0].ToString());
                }
                // Call Close when done reading.
                reader.Close();
            }

            

            foreach (var table in Table_list)
            {
                
                foreach (var LP in LP_list)
                {
                    QueryString = @"SELECT * FROM [CMA_Consistency].[Comp].[" + table + "] WHERE LP_LP = '" + LP + "'";
                    filepath = filepath_root + LP + @"\";
                    System.IO.Directory.CreateDirectory(filepath);
                    Console.WriteLine(filepath);
                    Filename = table+"_"+LP+".csv";
                    SQLtoCSV.SQLToCSV(connectionString, QueryString, filepath, Filename);
                }

            }
            

        }
    }
}
