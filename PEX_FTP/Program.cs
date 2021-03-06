﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.IO;
using System.Net;
using Renci.SshNet;
using Renci.SshNet.Common;
using Renci.SshNet.Sftp;
using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;

namespace PEX_FTP
{
    class Program
    {
        static void Main(string[] args)
        {
            string Op = args[0]; 
            string N = args[1];
            string Anio = args[2];
            string Periodo = args[3];

            //Se obtienen valores de configuración
            string ConString = ConfigurationManager.ConnectionStrings["SQLConnection"].ConnectionString;
            string FTPHost = ConfigurationManager.AppSettings["FTPHost"];
            string FTPUser = ConfigurationManager.AppSettings["FTPUser"];
            string FTPPass = ConfigurationManager.AppSettings["FTPPass"];
            int FTPPort = Int32.Parse(ConfigurationManager.AppSettings["FTPPort"]);
            string FTPDirectory = ConfigurationManager.AppSettings["FTPDirectory"];
            string Path = ConfigurationManager.AppSettings["PathFile"];
            
            //Para generar el nombre del archivo            
            string FileName;
            FileName = Name(Path,Op,N,Anio,Periodo);
            //

            Console.WriteLine("FTPServer: " + FTPHost);
            Console.WriteLine("FTPUser: " + FTPUser);
            Console.WriteLine("FTPPass: " + FTPPass);
            Console.WriteLine("FTPDirectory*: " + FTPDirectory);
            Console.WriteLine("Source: " + FileName);

            switch (Op)
            {
                case "1":
                    ConfirmationFile(ConString, FileName);
                    break;
                case "2":
                    GlobalResultsFile(ConString, FileName);
                    break;
                case "3":
                    FinanceFile(ConString, FileName);
                    break;
                case "4":
                    BankFile(ConString, FileName);
                    break;
                case "5":
                    FileName = "C:/Recibos/Zoetis/" + N;
                    break;
            }

            UploadFile(FTPHost, FTPUser, FTPPass, FTPPort, FTPDirectory, FileName);
            Console.ReadLine();
        }

        public static string Name (string Path,string Op,string N,string Anio,string Periodo)
        {
            string Date = DateTime.Now.ToString("yyyymmdd");
            string FileName="";

            if (Op=="1") //ConfirmationFile
            {
                if (N.Length > 0)
                {
                    FileName = Path + N;
                }
            }

            if (Op=="2") //GlobalResultsFile
            {
                FileName = Path + "GLREP_Z05_MX012_JJ_Y" + Anio + "_P" + Periodo + "_R01.csv";
            }

            if (Op == "3") //FinanceFile
            {
                FileName = Path + "FINANCE_Z05_MX012_JJ_Y" + Anio + "_P" + Periodo + "_R01.csv";
            }

            if (Op == "4") //BankFile
            {
                FileName = Path + "BANK_Z05_MX012_JJ_Y" + Anio + "_P" + Periodo + "_R01.txt";
            }

            return FileName;
        }

        public static void ConfirmationFile(string ConString,string FileName)
        {
            var CsvFile = FileName;
            try
            {
                using (SqlConnection conn = new SqlConnection(ConString))
                {
                    SqlCommand cmd = new SqlCommand();
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;
                    cmd.CommandText = "sp_ConfirmationLog_PEX";
                    cmd.Parameters.Add("@Enviar", SqlDbType.Int).Value = 1;
                    cmd.Connection = conn;
                    conn.Open();

                    System.Data.DataSet ds = new System.Data.DataSet();
                    System.Data.DataTable dt = new System.Data.DataTable();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(ds);
                    int c = ds.Tables[0].Rows.Count;
                    Console.WriteLine(c);

                   
                    File.WriteAllText(CsvFile, ds.Tables[0].Columns[0].ToString()+","+ ds.Tables[0].Columns[1].ToString()+"," 
                                              + ds.Tables[0].Columns[2].ToString() + ","+ds.Tables[0].Columns[3].ToString() + ","
                                              + ds.Tables[0].Columns[4].ToString() + "," + ds.Tables[0].Columns[5].ToString() + ","
                                              + ds.Tables[0].Columns[6].ToString() + "," + ds.Tables[0].Columns[7].ToString() //+ ","
                                              + Environment.NewLine);
                    for (int i = 0; i < c; i++)
                    {
                        File.AppendAllText(CsvFile, ds.Tables[0].Rows[i][0].ToString()+","+ ds.Tables[0].Rows[i][1].ToString() +","
                                                 + ds.Tables[0].Rows[i][2].ToString() + "," + ds.Tables[0].Rows[i][3].ToString() + ","
                                                 + ds.Tables[0].Rows[i][4].ToString() + "," + ds.Tables[0].Rows[i][5].ToString() + ","
                                                 + ds.Tables[0].Rows[i][6].ToString() + "," + ds.Tables[0].Rows[i][7].ToString() //+ ","
                                                 + Environment.NewLine);
                    }
                    
                    conn.Close();
                    Console.WriteLine("Archivo Generado.");
                }

            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }

        public static void GlobalResultsFile(string ConString, string FileName)
        {
            var CsvFile = FileName;
            try
            {
                using (SqlConnection conn = new SqlConnection(ConString))
                {
                    SqlCommand cmd = new SqlCommand();
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;
                    cmd.CommandText = "sp_Reporting_Global_PEX ";
                    cmd.Parameters.Add("@Enviar", SqlDbType.Int).Value = 1;
                    cmd.Connection = conn;
                    conn.Open();

                    System.Data.DataSet ds = new System.Data.DataSet();
                    System.Data.DataTable dt = new System.Data.DataTable();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(ds);
                    int c = ds.Tables[0].Rows.Count;
                    Console.WriteLine(c);


                    File.WriteAllText(CsvFile, ds.Tables[0].Columns[0].ToString() + "," + ds.Tables[0].Columns[1].ToString() + ","
                                              + ds.Tables[0].Columns[2].ToString() + "," + ds.Tables[0].Columns[3].ToString() + ","
                                              + ds.Tables[0].Columns[4].ToString() + "," + ds.Tables[0].Columns[5].ToString() + ","
                                              + ds.Tables[0].Columns[6].ToString() + "," + ds.Tables[0].Columns[7].ToString() + ","
                                              + ds.Tables[0].Columns[8].ToString() + Environment.NewLine);
                    for (int i = 0; i < c; i++)
                    {
                        File.AppendAllText(CsvFile, ds.Tables[0].Rows[i][0].ToString() + "," + ds.Tables[0].Rows[i][1].ToString() + ","
                                                 + ds.Tables[0].Rows[i][2].ToString() + "," + ds.Tables[0].Rows[i][3].ToString() + ","
                                                 + ds.Tables[0].Rows[i][4].ToString() + "," + ds.Tables[0].Rows[i][5].ToString() + ","
                                                 + ds.Tables[0].Rows[i][6].ToString() + "," + ds.Tables[0].Rows[i][7].ToString() + ","
                                                 + ds.Tables[0].Rows[i][8].ToString() + Environment.NewLine);
                    }

                    conn.Close();
                    Console.WriteLine("Archivo Generado.");
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }

        public static void FinanceFile(string ConString, string FileName)
        {
            var CsvFile = FileName;
            try
            {
                using (SqlConnection conn = new SqlConnection(ConString))
                {
                    SqlCommand cmd = new SqlCommand();
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;
                    cmd.CommandText = "sp_Finance_File_PEX";
                    cmd.Parameters.Add("@Enviar", SqlDbType.Int).Value = 1;
                    cmd.Connection = conn;
                    conn.Open();

                    System.Data.DataSet ds = new System.Data.DataSet();
                    System.Data.DataTable dt = new System.Data.DataTable();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(ds);
                    int c = ds.Tables[0].Rows.Count;
                    Console.WriteLine(c);


                    //File.WriteAllText(CsvFile, ds.Tables[0].Columns[0].ToString() + "," + ds.Tables[0].Columns[1].ToString() + ","
                    //                          + ds.Tables[0].Columns[2].ToString() + "," + ds.Tables[0].Columns[3].ToString() + ","
                    //                          + ds.Tables[0].Columns[4].ToString() + "," + ds.Tables[0].Columns[5].ToString() + ","
                    //                          + ds.Tables[0].Columns[6].ToString() + "," + ds.Tables[0].Columns[7].ToString() + ","
                    //                          + ds.Tables[0].Columns[8].ToString() + "," + ds.Tables[0].Columns[9].ToString() + ","
                    //                          + ds.Tables[0].Columns[10].ToString() + "," + ds.Tables[0].Columns[11].ToString() 
                    //                          + Environment.NewLine);

                    File.WriteAllText(CsvFile, ds.Tables[0].Columns[0].ToString() + "," + ds.Tables[0].Columns[1].ToString() + ","
                                              + ds.Tables[0].Columns[2].ToString() + "," + ds.Tables[0].Columns[3].ToString() + ","
                                              + ds.Tables[0].Columns[4].ToString() + "," + ds.Tables[0].Columns[5].ToString() + ","
                                              + ds.Tables[0].Columns[6].ToString() + Environment.NewLine);

                    for (int i = 0; i < c; i++)
                    {
                        File.AppendAllText(CsvFile, ds.Tables[0].Rows[i][0].ToString() + "," + ds.Tables[0].Rows[i][1].ToString() + ","
                                                 + ds.Tables[0].Rows[i][2].ToString() + "," + ds.Tables[0].Rows[i][3].ToString() + ","
                                                 + ds.Tables[0].Rows[i][4].ToString() + "," + ds.Tables[0].Rows[i][5].ToString() + ","
                                                 + ds.Tables[0].Rows[i][6].ToString() + "," + ds.Tables[0].Rows[i][7].ToString() + ","
                                                 + ds.Tables[0].Rows[i][8].ToString() + "," + ds.Tables[0].Rows[i][9].ToString() + ","
                                                 + ds.Tables[0].Rows[i][10].ToString() + "," + ds.Tables[0].Rows[i][11].ToString()
                                                 + Environment.NewLine);
                    }

                    conn.Close();
                    Console.WriteLine("Archivo Generado.");
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }

        public static void BankFile(string ConString, string FileName)
        {
            var txtFile = FileName;
            try
            {
                using (SqlConnection conn = new SqlConnection(ConString))
                {
                    SqlCommand cmd = new SqlCommand();
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;
                    cmd.CommandText = "sp_BankFile_PEX";
                    cmd.Parameters.Add("@Enviar", SqlDbType.Int).Value = 1;
                    cmd.Connection = conn;
                    conn.Open();

                    System.Data.DataSet ds = new System.Data.DataSet();
                    System.Data.DataTable dt = new System.Data.DataTable();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(ds);
                    int c = ds.Tables[0].Rows.Count;
                    Console.WriteLine(c);


                    //File.WriteAllText(txtFile, ds.Tables[0].Columns[0].ToString() + Environment.NewLine);

                    for (int i = 0; i < c; i++)
                    {
                        File.AppendAllText(txtFile, ds.Tables[0].Rows[i][0].ToString() + Environment.NewLine);
                    }

                    conn.Close();
                    Console.WriteLine("Archivo Generado.");
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }

        public static void UploadFile(string host,string user,string pass,int port,string directory,string FileName)
        {
            string uploadfile = FileName;
            try
            {
                Console.WriteLine("Creating client and connecting");
                using (var client = new SftpClient(host, port, user, pass))
                {
                    client.Connect();
                    Console.WriteLine("Connected to {0}", host);

                    client.ChangeDirectory(directory);
                    Console.WriteLine("Changed directory to {0}", directory);

                    //var listDirectory = client.ListDirectory(directory);
                    //Console.WriteLine("Listing directory:");
                    //foreach (var fi in listDirectory)
                    //{
                    //    Console.WriteLine(" - " + fi.Name);
                    //}

                    using (var fileStream = new FileStream(uploadfile, FileMode.Open))
                    {
                        Console.WriteLine("Uploading {0} ({1:N0} bytes)", uploadfile, fileStream.Length);
                        client.BufferSize = 4 * 1024; // bypass Payload error large files
                        client.UploadFile(fileStream, Path.GetFileName(uploadfile));
                        Console.WriteLine(Path.GetFileName(uploadfile));
                    }
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }



     
    }
}
