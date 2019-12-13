using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Threading;
using System.Diagnostics;
using MySql.Data.MySqlClient;

namespace Catalog
{
    public partial class Form1 : Form
    {
        MySqlConnection connection;

        public Form1()
        {
            InitializeComponent();

            StartServer();
        }

        public void StartServer()
        {
            //Process.Start(@"C:\WebServers\denwer\Run.exe");
            //Thread.Sleep(3000);
        }

        public void CreateDatabase()
        {
            try
            {
                OpenConnection(String.Format("server={0};user={1};password={2};database={3};CharSet=utf8;Convert Zero Datetime=True;",
                    textBox1.Text, textBox2.Text, textBox3.Text, textBox4.Text));
                MessageBox.Show("База данных уже существует");
                ExecuteCommand(string.Format("use {0};", textBox4.Text));
            }
            catch
            {
                OpenConnection(String.Format("server={0};user={1};password={2};CharSet=utf8;Convert Zero Datetime=True;",
                    textBox1.Text, textBox2.Text, textBox3.Text));

                ExecuteCommand(string.Format("create database {0} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;", textBox4.Text));

                CreateTables();
            }
        }

        public void CreateTables()
        {
            ExecuteCommand(string.Format("use {0};", textBox4.Text));
            ExecuteCommand(string.Format("create table catalog (id int primary key auto_increment, name varchar(50) " +
                "not null default 'unknown', description varchar(200) not null default 'no description');"));
            ExecuteCommand(string.Format("create table catalog_aggregate (id int primary key auto_increment, " +
                "name varchar(50) not null default 'unknown', description varchar(200) not null default 'no description', url varchar(200) not null default 'unknown', " +
                "catalog_id int null, foreign key (catalog_id) references catalog (id));"));
            ExecuteCommand(string.Format("create table catalog_model (id int primary key auto_increment, model varchar(50) not null default 'unknown', " +
                "description varchar(200) not null default 'no description', url varchar(200) not null default 'unknown', catalog_aggregate_id int null, " +
                "foreign key (catalog_aggregate_id) references catalog_aggregate (id));"));
            ExecuteCommand(string.Format("create table catalog_level (id int primary key auto_increment, parent_id int, " +
                "name varchar(50) not null default 'unknown', description varchar(200) not null default 'no description);"));
        }

        public void AddValuesToCatalog()
        {
            bool values_exist = false;

            string query = "insert into catalog set ";

            if (!String.IsNullOrWhiteSpace(catalog_name_text.Text))
            {
                query += "name= '" + catalog_name_text.Text + "'";
                values_exist = true;
            }
            if (!String.IsNullOrWhiteSpace(catalog_description_text.Text))
            {
                if (values_exist)
                    query += ", ";
                query += "description= '" + catalog_description_text.Text + "'";
                values_exist = true;
            }
            query += ";";
            if (values_exist)
                ExecuteCommand(query);
            else
                ExecuteCommand("insert into catalog values ();");
        }

        public void AddValuesToCatalogAggregate()
        {
            bool values_exist = false;

            string query = "insert into catalog_aggregate set ";

            if (!String.IsNullOrWhiteSpace(catalog_aggregate_name_text.Text))
            {
                query += "name= '" + catalog_aggregate_name_text.Text + "'";
                values_exist = true;
            }
            if (!String.IsNullOrWhiteSpace(catalog_aggregate_description_text.Text))
            {
                if (values_exist)
                    query += ", ";
                query += "description= '" + catalog_aggregate_description_text.Text + "'";
                values_exist = true;
            }
            if (!String.IsNullOrWhiteSpace(catalog_aggregate_url_text.Text))
            {
                if (values_exist)
                    query += ", ";
                query += "url= '" + catalog_aggregate_url_text.Text + "'";
                values_exist = true;
            }
            if (!String.IsNullOrWhiteSpace(catalog_aggregate_catalog_id_text.Text))
            {
                if (values_exist)
                    query += ", ";
                query += "catalog_id= " + catalog_aggregate_catalog_id_text.Text;
                values_exist = true;
            }
            query += ";";
            if (values_exist)
                ExecuteCommand(query);
            else
                ExecuteCommand("insert into catalog_aggregate values ();");
        }

        public void AddValuesToCatalogModel()
        {
            bool values_exist = false;

            string query = "insert into catalog_model set ";

            if (!String.IsNullOrWhiteSpace(catalog_model_model_text.Text))
            {
                query += "model= '" + catalog_model_model_text.Text + "'";
                values_exist = true;
            }
            if (!String.IsNullOrWhiteSpace(catalog_model_description_text.Text))
            {
                if (values_exist)
                    query += ", ";
                query += "description= '" + catalog_model_description_text.Text + "'";
                values_exist = true;
            }
            if (!String.IsNullOrWhiteSpace(catalog_model_url_text.Text))
            {
                if (values_exist)
                    query += ", ";
                query += "url= '" + catalog_model_url_text.Text + "'";
                values_exist = true;
            }
            if (!String.IsNullOrWhiteSpace(catalog_model_catalog_aggregate_id_text.Text))
            {
                if (values_exist)
                    query += ", ";
                query += "catalog_aggregate_id= " + catalog_model_catalog_aggregate_id_text.Text;
                values_exist = true;
            }
            query += ";";
            if (values_exist)
                ExecuteCommand(query);
            else
                ExecuteCommand("insert into catalog_model values ();");
        }

        public void AddValuesToCatalogLevel()
        {

        }

        public void ShowCatalogTable(string table_name, int number_of_columns, DataGridView dataGridView)
        {
            dataGridView.Rows.Clear();
            dataGridView.Refresh();

            ExecuteCommand("SET NAMES cp1251");
            List<string[]> data = ExecuteReader(string.Format("select * from {0};", table_name), number_of_columns);

            try
            {
                foreach (string[] s in data)
                    dataGridView.Rows.Add(s);
            }
            catch
            {
                MessageBox.Show("Таблица пуста");
            }
        }

        private void button1_Click(object sender, EventArgs e)
        {
            CreateDatabase();
            ShowCatalogTable("catalog", 3, dataGridView1);
            ShowCatalogTable("catalog_aggregate", 5, dataGridView2);
            ShowCatalogTable("catalog_model", 5, dataGridView3);
        }

        private void ExecuteCommand(string query)
        {
            MySqlCommand command = new MySqlCommand(query, connection);
            command.ExecuteNonQuery();
        }

        private List<string[]> ExecuteReader(string query, int number_of_columns)
        {
            MySqlCommand command = new MySqlCommand(query, connection);
            MySqlDataReader reader = command.ExecuteReader();

            List<string[]> data = new List<string[]>();

            while (reader.Read())
            {
                data.Add(new string[number_of_columns]);
                int c_number = 0;

                for (int i = 0; i < number_of_columns; i++)
                    data[data.Count - 1][c_number++] = reader[i].ToString();
            }

            reader.Close();

            return data;
        }

        private void OpenConnection(string connection_string)
        {
            connection = new MySqlConnection(connection_string);
            connection.Open();
        }

        private void catalog_add_button_Click(object sender, EventArgs e)
        {
            AddValuesToCatalog();
            ShowCatalogTable("catalog", 3, dataGridView1);
        }

        private void catalog_aggregate_add_button_Click(object sender, EventArgs e)
        {
            AddValuesToCatalogAggregate();
            ShowCatalogTable("catalog_aggregate", 5, dataGridView2);
        }

        private void catalog_model_add_button_Click(object sender, EventArgs e)
        {
            AddValuesToCatalogModel();
            ShowCatalogTable("catalog_model", 5, dataGridView3);
        }
    }
}
