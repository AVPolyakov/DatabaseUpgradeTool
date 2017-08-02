using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using Microsoft.SqlServer.TransactSql.ScriptDom;


namespace DatabaseVersioningTool
{
    public class DBHelper
    {
        private readonly string _connectionString;

        public DBHelper(string connectionString)
        {
            _connectionString = connectionString;
        }

        public T ExecuteScalar<T>(string commandText, params SqlParameter[] parameters)
        {
            using (SqlConnection cnn = new SqlConnection(_connectionString))
            {
                SqlCommand cmd = new SqlCommand(commandText, cnn)
                {
                    CommandType = CommandType.Text
                };
                foreach (SqlParameter parameter in parameters)
                {
                    cmd.Parameters.Add(parameter);
                }

                cnn.Open();

                return (T)cmd.ExecuteScalar();
            }
        }

        public void ExecuteNonQuery(string commandText, params SqlParameter[] parameters)
        {
            using (SqlConnection cnn = new SqlConnection(_connectionString))
            {
                SqlCommand cmd = new SqlCommand(commandText, cnn)
                {
                    CommandType = CommandType.Text
                };
                foreach (SqlParameter parameter in parameters)
                {
                    cmd.Parameters.Add(parameter);
                }

                cnn.Open();
                cmd.ExecuteNonQuery();
            }
        }

        public void ExecuteMigration(string commandText)
        {
            var subCommands = GetBatches(commandText);

            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                SqlTransaction transaction = connection.BeginTransaction();
                using (SqlCommand cmd = connection.CreateCommand())
                {
                    cmd.Connection = connection;
                    cmd.Transaction = transaction;

                    foreach (string command in subCommands)
                    {
                        if (command.Length <= 0)
                            continue;

                        cmd.CommandText = command;
                        cmd.CommandType = CommandType.Text;

                        try
                        {
                            cmd.ExecuteNonQuery();
                        }
                        catch (SqlException)
                        {
                            transaction.Rollback();
                            throw;
                        }
                    }
                }

                transaction.Commit();
            }
        }

        private static IEnumerable<string> GetBatches(string text)
        {
            var parser = new TSql120Parser(false);
            IList<ParseError> parseErrors;
            var scriptFragment = parser.Parse(new StringReader(text), out parseErrors);
            if (parseErrors.Count > 0)
            {
                var error = parseErrors[0];
                throw new ApplicationException($@"{error.Message}
Line={error.Line},
Column={error.Column}");
            }
            if (!(scriptFragment is TSqlScript))
                throw new ApplicationException();
            var sqlScript = (TSqlScript) scriptFragment;
            foreach (var sqlBatch in sqlScript.Batches)
                yield return text.Substring(sqlBatch.StartOffset, sqlBatch.FragmentLength);
        }
    }
}
