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
                "name varchar(50) not null default 'unknown', description varchar(200) not null default 'no description');"));
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

        public void AddValuesToCatalogLevelV1(List<string[]> categories)
        {
            string current_category;
            int index = 0; // предполагается, что таблица очищается после каждого вызова, поэтому можно брать индекс из программы. В противном случае необходимо вызывать count(*) в самой таблице
            Dictionary<string, int> indexes = new Dictionary<string, int>();
            Queue<string> parents_queue = new Queue<string>(GetTopParents(categories));
            List<string[]> children_data;
            List<string[]> data;
            List<string[]> reference_data;
            int parent_id;
            string parent_name;
            List<string> available_categories = new List<string>();
            foreach (string[] c in categories)
                available_categories.Add(c[0].ToString());

            ExecuteCommand("delete from catalog_level");
            ExecuteCommand("ALTER TABLE catalog_level AUTO_INCREMENT=1");

            while (parents_queue.Count > 0)
            {
                current_category = parents_queue.Dequeue();
                available_categories.Remove(current_category.ToString());

                reference_data = ExecuteReader(String.Format("select column_name, referenced_table_name, referenced_column_name " +
                        "from information_schema.key_column_usage where table_name = '{0}' and constraint_name like '%_ibfk_%';", current_category), 3);
                if (reference_data.Count == 0)
                {
                    data = ExecuteReader(String.Format("select id, name, description from {0};", current_category), 3);
                    try
                    {
                        foreach (string[] d in data)
                        {
                            ExecuteCommand(String.Format("insert into catalog_level set name = '{0}', description = '{1}';", d[1], d[2]));
                            indexes.Add(String.Format("{0}.{1}", current_category, d[0]), ++index);
                        }
                    }
                    catch { }
                }
                else
                {
                    //проблема, возникающая из-за того, что в catalog_model указано model, а не name. Пока не вижу, как сделать такой вывод универсальным (возможно, выводить всё и ориентироваться строго на вторую колонку, но тоже может создавать проблемы).
                    try
                    {
                        data = ExecuteReader(String.Format("select id, name, description, {0} from {1};", reference_data[0][0], current_category), 4);
                    }
                    catch
                    {
                        data = ExecuteReader(String.Format("select id, model, description, {0} from {1};", reference_data[0][0], current_category), 4);
                    }
                    try
                    {
                        foreach (string[] d in data)
                        {
                            parent_name = String.Format("{0}.{1}", reference_data[0][1], d[3]);
                            if (indexes.ContainsKey(parent_name))
                            {
                                parent_id = indexes[parent_name];
                                ExecuteCommand(String.Format("insert into catalog_level set parent_id = {0}, name = '{1}', description = '{2}';", parent_id, d[1], d[2]));
                            }
                            else
                                ExecuteCommand(String.Format("insert into catalog_level set name = '{0}', description = '{1}';", d[1], d[2]));

                            indexes.Add(String.Format("{0}.{1}", current_category, d[0]), ++index);
                        }
                    }
                    catch { }
                }

                children_data = ExecuteReader(String.Format("select table_name " +
                        "from information_schema.key_column_usage where referenced_table_name = '{0}' and constraint_name like '%_ibfk_%';", current_category), 1);

                foreach (string[] c in children_data)
                {
                    if (available_categories.Contains(c[0].ToString()))
                        parents_queue.Enqueue(c[0].ToString());
                }
            }
        }

        public void AddValuesToCatalogLevelV2(List<string[]> categories)
        {
            string current_category;
            int index = 0; // предполагается, что таблица очищается после каждого вызова, поэтому можно брать индекс из программы. В противном случае необходимо вызывать count(*) в самой таблице
            Dictionary<string, int> indexes = new Dictionary<string, int>();
            Stack<string> parents_stack = new Stack<string>(GetTopParents(categories));
            List<string[]> children_data;
            List<string[]> data;
            List<string[]> reference_data;
            int parent_id;
            string parent_name;
            List<string> available_categories = new List<string>();
            foreach (string[] c in categories)
                available_categories.Add(c[0].ToString());

            ExecuteCommand("delete from catalog_level");
            ExecuteCommand("ALTER TABLE catalog_level AUTO_INCREMENT=1");

            while (parents_stack.Count > 0)
            {
                current_category = parents_stack.Pop();
                available_categories.Remove(current_category.ToString());

                reference_data = ExecuteReader(String.Format("select column_name, referenced_table_name, referenced_column_name " +
                        "from information_schema.key_column_usage where table_name = '{0}' and constraint_name like '%_ibfk_%';", current_category), 3);
                if (reference_data.Count == 0)
                {
                    data = ExecuteReader(String.Format("select id, name, description from {0};", current_category), 3);
                    try
                    {
                        foreach (string[] d in data)
                        {
                            ExecuteCommand(String.Format("insert into catalog_level set name = '{0}', description = '{1}';", d[1], d[2]));
                            indexes.Add(String.Format("{0}.{1}", current_category, d[0]), ++index);
                        }
                    }
                    catch { }
                }
                else
                {
                    //проблема, возникающая из-за того, что в catalog_model указано model, а не name. Пока не вижу, как сделать такой вывод универсальным (возможно, выводить всё и ориентироваться строго на вторую колонку, но тоже может создавать проблемы).
                    try
                    {
                        data = ExecuteReader(String.Format("select id, name, description, {0} from {1};", reference_data[0][0], current_category), 4);
                    }
                    catch
                    {
                        data = ExecuteReader(String.Format("select id, model, description, {0} from {1};", reference_data[0][0], current_category), 4);
                    }
                    try
                    {
                        foreach (string[] d in data)
                        {
                            parent_name = String.Format("{0}.{1}", reference_data[0][1], d[3]);
                            if (indexes.ContainsKey(parent_name))
                            {
                                parent_id = indexes[parent_name];
                                ExecuteCommand(String.Format("insert into catalog_level set parent_id = {0}, name = '{1}', description = '{2}';", parent_id, d[1], d[2]));
                            }
                            else
                                ExecuteCommand(String.Format("insert into catalog_level set name = '{0}', description = '{1}';", d[1], d[2]));

                            indexes.Add(String.Format("{0}.{1}", current_category, d[0]), ++index);
                        }
                    }
                    catch { }
                }

                children_data = ExecuteReader(String.Format("select table_name " +
                        "from information_schema.key_column_usage where referenced_table_name = '{0}' and constraint_name like '%_ibfk_%';", current_category), 1);

                foreach (string[] c in children_data)
                {
                    if (available_categories.Contains(c[0].ToString()))
                        parents_stack.Push(c[0].ToString());
                }
            }

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

        public List<string> GetTopParents(List<string[]> categories)
        {
            List<int> levels = new List<int>();
            List<string> top_parents = new List<string>();

            int minimum_level = Int32.MaxValue;

            int number_of_level;

            foreach (string[] s in categories)
            {
                number_of_level = 0;
                List<string[]> data = ExecuteReader(string.Format("select referenced_table_name from information_schema.key_column_usage " +
                    "where table_name = '{0}' and constraint_name like '%_ibfk_%';", s), 1);
                while (data.Any())
                {
                    number_of_level++;
                    data = ExecuteReader(string.Format("select referenced_table_name from information_schema.key_column_usage " +
                    "where table_name = '{0}' and constraint_name like '%_ibfk_%';", data[0]), 1);
                }

                levels.Add(number_of_level);

                minimum_level = number_of_level < minimum_level ? number_of_level : minimum_level;
            }

            int parent_index;

            while (levels.Contains(minimum_level))
            {
                parent_index = levels.IndexOf(minimum_level);
                top_parents.Add(categories[parent_index][0].ToString());
                levels.RemoveAt(parent_index);
                categories.RemoveAt(parent_index);
            }

            return top_parents;
        }

        public void UpdateCatalogLevel()
        {
            if (alternative_fulling_check_box.Checked)
                AddValuesToCatalogLevelV1(new List<string[]>() { new string[] { "catalog" }, new string[] { "catalog_aggregate" }, new string[] { "catalog_model" } });
            else
                AddValuesToCatalogLevelV2(new List<string[]>() { new string[] { "catalog" }, new string[] { "catalog_aggregate" }, new string[] { "catalog_model" } });
            ShowCatalogTable("catalog_level", 4, dataGridView4);
        }

        private void button1_Click(object sender, EventArgs e)
        {
            CreateDatabase();
            ShowCatalogTable("catalog", 3, dataGridView1);
            ShowCatalogTable("catalog_aggregate", 5, dataGridView2);
            ShowCatalogTable("catalog_model", 5, dataGridView3);
            UpdateCatalogLevel();
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
            UpdateCatalogLevel();
        }

        private void catalog_aggregate_add_button_Click(object sender, EventArgs e)
        {
            AddValuesToCatalogAggregate();
            ShowCatalogTable("catalog_aggregate", 5, dataGridView2);
            UpdateCatalogLevel();
        }

        private void catalog_model_add_button_Click(object sender, EventArgs e)
        {
            AddValuesToCatalogModel();
            ShowCatalogTable("catalog_model", 5, dataGridView3);
            UpdateCatalogLevel();
        }

        private void button2_Click(object sender, EventArgs e)
        {
            AddValuesToCatalogLevelV1(new List<string[]>() { new string[] { "catalog" }, new string[] { "catalog_aggregate" }, new string[] { "catalog_model" } });
        }
    }
}
