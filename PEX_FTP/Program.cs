using System;
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
using System.Data.Sql;
using System.Data.SqlClient;

namespace PEX_FTP
{
    class Program
    {
        static void Main(string[] args)
        {
            //WAITINGEVENTS_Z05_MX012_JJ_D20160413_O002

            //Se obtienen valores de configuración
            string ConString = ConfigurationManager.ConnectionStrings["SQLConnection"].ConnectionString;
            string FTPHost = ConfigurationManager.AppSettings["FTPHost"];
            string FTPUser = ConfigurationManager.AppSettings["FTPUser"];
            string FTPPass = ConfigurationManager.AppSettings["FTPPass"];
            int FTPPort = Int32.Parse(ConfigurationManager.AppSettings["FTPPort"]);
            string FTPDirectory = ConfigurationManager.AppSettings["FTPDirectory"];

            //Se genera nombre de archivo
            string Path = "C:/Users/Aliatec-HP/Desktop/";
            string Number = "";
            string Date = DateTime.Now.ToString("yyyymmdd");

            
            if (args.Length>0)
            {
                Number = args[0];
                if (Number.Length.Equals(1))
                {
                    Number = "00" + Number;
                }
                if (Number.Length.Equals(2))
                {
                    Number = "0" + Number;
                }
            }
            else
            {
                Number = "001";
            }         
            
                  
            string FileName = Path+"WAITINGEVENTS_Z05_MX012_JJ_D"+ Date + "_O"+Number+".csv";


            Console.WriteLine("FTPServer: " + FTPHost);
            Console.WriteLine("FTPUser: " + FTPUser);
            Console.WriteLine("FTPPass: " + FTPPass);
            Console.WriteLine("FTPDirectory: " + FTPDirectory);
            Console.WriteLine("Source: " + FileName);
                        
            ConfirmationFile(ConString,FileName);
            UploadFile(FTPHost, FTPUser, FTPPass, FTPPort, FTPDirectory,FileName);

            Console.ReadLine();
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
                                              + ds.Tables[0].Columns[6].ToString() + "," + ds.Tables[0].Columns[7].ToString() + ","
                                              + Environment.NewLine);
                    for (int i = 0; i < c; i++)
                    {
                        File.AppendAllText(CsvFile, ds.Tables[0].Rows[i][0].ToString()+","+ ds.Tables[0].Rows[i][1].ToString() +","
                                                 + ds.Tables[0].Rows[i][2].ToString() + "," + ds.Tables[0].Rows[i][3].ToString() + ","
                                                 + ds.Tables[0].Rows[i][4].ToString() + "," + ds.Tables[0].Rows[i][5].ToString() + ","
                                                 + ds.Tables[0].Rows[i][6].ToString() + "," + ds.Tables[0].Rows[i][7].ToString() + ","
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


        public static void UploadFile(string host,string user,string pass,int port,string directory,string FileName)
        {
            string uploadfile = FileName;
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



     
    }
}
