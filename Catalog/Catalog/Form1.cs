using System;
using System.Collections.Generic;
using System.Linq;
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

        /// <summary>
        /// Запуск Денвера для связи с базой MySQL
        /// </summary>
        public void StartServer()
        {
            Process.Start(@"C:\WebServers\denwer\Run.exe");
            Thread.Sleep(3000);
        }

        /// <summary>
        /// Создание базы данных
        /// </summary>
        public void CreateDatabase()
        {
            // если базы данных нет, создаётся она и все необходимые таблицы; иначе выбирается указанная база данных
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

        /// <summary>
        /// Создание структуры таблиц
        /// </summary>
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

        /// <summary>
        /// Получение доступа к кнопкам
        /// </summary>
        public void ButtonsEnabled()
        {
            catalog_add_button.Enabled = true;
            catalog_aggregate_add_button.Enabled = true;
            catalog_model_add_button.Enabled = true;
        }

        /// <summary>
        /// Добавление значений в таблицу catalog
        /// </summary>
        public void AddValuesToCatalog()
        {

            bool values_exist = false;

            string query = "insert into catalog set ";

            // добавление элементов; если соответствующая форма пуста, значение не добавляется (будет установлено дефолтное)
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

        /// <summary>
        /// Добавление значений в таблицу catalog_aggregate
        /// </summary>
        public void AddValuesToCatalogAggregate()
        {
            bool values_exist = false;

            string query = "insert into catalog_aggregate set ";

            // добавление элементов; если соответствующая форма пуста, значение не добавляется (будет установлено дефолтное)
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

        /// <summary>
        /// Добавление значений в таблицу catalog_model
        /// </summary>
        public void AddValuesToCatalogModel()
        {

            bool values_exist = false;

            string query = "insert into catalog_model set ";

            // добавление элементов; если соответствующая форма пуста, значение не добавляется (будет установлено дефолтное)
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

        /// <summary>
        /// Добавление значений в таблицу catalog_level.
        /// Реализует последовательное добавление: от элементов с наименьшим количеством родителей до элементов с наибольшим
        /// </summary>
        /// <param name="categories">Список категорий(таблиц), которые должны быть добавлены в таблицу.</param>
        public void AddValuesToCatalogLevelV1(List<string[]> categories)
        {
            string current_category;
            int index = 0; // предполагается, что таблица очищается после каждого вызова, поэтому можно брать индекс из программы. В противном случае необходимо вызывать count(*) из таблицы
            Dictionary<string, int> indexes = new Dictionary<string, int>(); // Структура "table_name.index" = index для получения соответствия элемента в исходной таблице и индекса в созданной
            Queue<string> parents_queue = new Queue<string>(GetTopParents(categories)); // начальная очередь состоит из элементов с наименьшим числом родителей
            List<string[]> children_data; 
            List<string[]> data; 
            List<string[]> reference_data;
            int parent_id;
            string parent_name;
            List<string> available_categories = new List<string>(); // список ещё не задействованных таблиц. Введён для предотвращения невозможности добавления элементов, не имеющих указанного в создаваемой таблице родителя и для предотавращения закольцованности (а вдруг)
            foreach (string[] c in categories)
                available_categories.Add(c[0].ToString());

            ClearTable("catalog_level");

            // пока не закончена очередь, каждый элемент заносится в catalog_level с указанием родителя (предполагается только 1 родитель)
            while (parents_queue.Count > 0)
            {
                current_category = parents_queue.Dequeue();
                available_categories.Remove(current_category.ToString());

                // поиск родителей
                reference_data = ExecuteReader(String.Format("select column_name, referenced_table_name, referenced_column_name " +
                        "from information_schema.key_column_usage where table_name = '{0}' and constraint_name like '%_ibfk_%';", current_category), 3);

                // если родителей нет, родитель не указывается. Если родители есть, но они не указаны в таблице, также не указывается родитель. Иначе - указывается согласно индексу, указанному в словаре indexes
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

                // получение информации о потомках
                children_data = ExecuteReader(String.Format("select table_name " +
                        "from information_schema.key_column_usage where referenced_table_name = '{0}' and constraint_name like '%_ibfk_%';", current_category), 1);

                // потомки добавляются в очередь
                foreach (string[] c in children_data)
                {
                    if (available_categories.Contains(c[0].ToString()))
                        parents_queue.Enqueue(c[0].ToString());
                }

                // предотвращение потери данных, имеющих родителей, но не указанных в categories
                if (parents_queue.Count == 0 && available_categories.Count != 0)
                {
                    List<string[]> crutch = new List<string[]>();
                    foreach (string a in available_categories)
                        crutch.Add(new string[] { a });
                    parents_queue = new Queue<string>(GetTopParents(crutch));
                }
            }
        }

        /// <summary>
        /// Добавление значений в таблицу catalog_level.
        /// Реализует древовидное добавление: сначала добавляются потомки одного элемента, затем другого и т.д.
        /// Имеет абсолютно ту же структуру, но вместо очереди используется стек.
        /// </summary>
        /// <param name="categories"></param>
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

            ClearTable("catalog_level");

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

                if (parents_stack.Count == 0 && available_categories.Count != 0)
                {
                    List<string[]> crutch = new List<string[]>();
                    foreach (string a in available_categories)
                        crutch.Add(new string[] { a });
                    parents_stack = new Stack<string>(GetTopParents(crutch));
                }
            }

        }

        /// <summary>
        /// Очистка таблицы.
        /// Используется для заполнения таблицы с самого начала со сбросом счётчика индексов.
        /// </summary>
        /// <param name="table_name">Имя таблицы, для которой будет произведён сброс</param>
        public void ClearTable(string table_name)
        {
            ExecuteCommand(string.Format("delete from {0}", table_name));
            ExecuteCommand(string.Format("ALTER TABLE {0} AUTO_INCREMENT=1", table_name));
        }

        /// <summary>
        /// Вывод данных в таблицу
        /// </summary>
        /// <param name="table_name">Имя таблицы, которая будет выведена.</param>
        /// <param name="number_of_columns">Число столбцов выводимой таблицы.</param>
        /// <param name="dataGridView">Элемент, в который будет выведена таблица.</param>
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

        /// <summary>
        /// Получение названий таблиц (категорий) с наименьшим числом родителей.
        /// </summary>
        /// <param name="categories">Названия таблиц, среди которых проводится поиск.</param>
        /// <returns>Список элементов с наименьшим числом родителей.</returns>
        public List<string> GetTopParents(List<string[]> categories)
        {
            List<int> levels = new List<int>(); //число родительских элементов
            List<string> top_parents = new List<string>(); //список элементов с наименьшим числом родителей

            int minimum_level = Int32.MaxValue;

            int number_of_level;

            // поиск количества родительских элементов для каждой из таблиц и определение наименьшего числа родительских уровней среди всех полученных
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

            // получение элементов с наименьшим числом родителей
            while (levels.Contains(minimum_level))
            {
                parent_index = levels.IndexOf(minimum_level);
                top_parents.Add(categories[parent_index][0].ToString());
                levels.RemoveAt(parent_index);
                categories.RemoveAt(parent_index);
            }

            return top_parents;
        }

        /// <summary>
        /// Обновление таблицы catalog_level, таблицы, отображаемой на форме, и элемента treeView
        /// </summary>
        public void UpdateCatalogLevel()
        {
            if (alternative_fulling_check_box.Checked)
                AddValuesToCatalogLevelV2(new List<string[]>() { new string[] { "catalog" }, new string[] { "catalog_aggregate" }, new string[] { "catalog_model" } });
            else
                AddValuesToCatalogLevelV1(new List<string[]>() { new string[] { "catalog" }, new string[] { "catalog_aggregate" }, new string[] { "catalog_model" } });
            ShowCatalogTable("catalog_level", 4, dataGridView4);
            UpdateTreeView();
        }

        /// <summary>
        /// Обновление элемента treeView.
        /// Предполагается, что catalog_tree сохраняет иерархичность; родитель элементов, чьи родители не представлены в таблице не указаны.
        /// </summary>
        public void UpdateTreeView()
        {
            TreeNode string_node;
            string parent_id;

            catalog_level_tree.Nodes.Clear();

            List<string[]> catalog_level_data = ExecuteReader("select * from catalog_level", 4);

            // для каждого узла ищется родитель согласно id родителя, который указывается в качестве имени узла
            foreach (string[] c in catalog_level_data)
            {
                // если родитель не указан, узел устанавливается корневымl иначе производится обход всех узлов в поисках родителя и установка текущего элемента в качестве дочернего
                if (c[1].Count() == 0)
                {
                    string_node = new TreeNode { Text = c[2], Name = c[0] };
                    catalog_level_tree.Nodes.Add(string_node);
                }
                else
                {
                    parent_id = c[1];
                    foreach (TreeNode tn in catalog_level_tree.Nodes)
                    {
                        if (tn.Name == parent_id)
                        {
                            tn.Nodes.Add(new TreeNode { Text = c[2], Name = c[0] });
                            break;
                        }
                        else
                            CheckChildNodes(tn, parent_id, c[2], c[0]);
                    }
                    
                }
            }
        }

        /// <summary>
        /// Обход всех дочерних узлов дерева.
        /// Использует рекурсию для обхода.
        /// </summary>
        /// <param name="parent">Родительский узел</param>
        /// <param name="parent_id">ID родительского элемента</param>
        /// <param name="text">Устанавливаемое название, отображаемое в treeView</param>
        /// <param name="name">Имя узла, фактически id элемента</param>
        public void CheckChildNodes(TreeNode parent, string parent_id, string text, string name)
        {
            foreach (TreeNode tn in parent.Nodes)
            {
                if (tn.Name == parent_id)
                    tn.Nodes.Add(new TreeNode { Text = text, Name = name });
                else
                    CheckChildNodes(tn, parent_id, text, name);
            }
        }

        /// <summary>
        /// Обработчик нажатия кнопки.
        /// Запускает функции создания базы данных, отображения таблиц в форме, обновления данных таблицы catalog_level и элемента treeView
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button1_Click(object sender, EventArgs e)
        {
            CreateDatabase();
            ShowCatalogTable("catalog", 3, dataGridView1);
            ShowCatalogTable("catalog_aggregate", 5, dataGridView2);
            ShowCatalogTable("catalog_model", 5, dataGridView3);
            UpdateCatalogLevel();
            ButtonsEnabled();
        }

        /// <summary>
        /// Отправка команд MySQL серверу
        /// </summary>
        /// <param name="query">Команда, передаваемая MySQL серверу.</param>
        private void ExecuteCommand(string query)
        {
            string query_ = "SET NAMES utf8";
            MySqlCommand command_ = new MySqlCommand(query_, connection);
            command_.ExecuteNonQuery();

            MySqlCommand command = new MySqlCommand(query, connection);
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// Запрос данных от MySQL сервера.
        /// </summary>
        /// <param name="query">Комнада MySQL серверу.</param>
        /// <param name="number_of_columns">Число принимаемых колонок</param>
        /// <returns>Список данных от MySQL данных</returns>
        private List<string[]> ExecuteReader(string query, int number_of_columns)
        {
            ExecuteCommand("SET NAMES cp1251");
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

        /// <summary>
        /// Подключение к MySQL серверу.
        /// </summary>
        /// <param name="connection_string">Запрос на подключение.</param>
        private void OpenConnection(string connection_string)
        {
            connection = new MySqlConnection(connection_string);
            connection.Open();
        }

        /// <summary>
        /// Обработчик нажания кнопки.
        /// Добавление данных в таблицу catalog и обновление таблицы catalog_level.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void catalog_add_button_Click(object sender, EventArgs e)
        {
            AddValuesToCatalog();
            ShowCatalogTable("catalog", 3, dataGridView1);
            UpdateCatalogLevel();
        }

        /// <summary>
        /// Обработчик нажания кнопки.
        /// Добавление данных в таблицу catalog_aggregate и обновление таблицы catalog_level.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void catalog_aggregate_add_button_Click(object sender, EventArgs e)
        {
            AddValuesToCatalogAggregate();
            ShowCatalogTable("catalog_aggregate", 5, dataGridView2);
            UpdateCatalogLevel();
        }

        /// <summary>
        /// Обработчик нажания кнопки.
        /// Добавление данных в таблицу catalog и обновление таблицы catalog_level.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void catalog_model_add_button_Click(object sender, EventArgs e)
        {
            AddValuesToCatalogModel();
            ShowCatalogTable("catalog_model", 5, dataGridView3);
            UpdateCatalogLevel();
        }

        /// <summary>
        /// Обработчик нажатия кнопки.
        /// Была сделана для тестовых функций. Сейчас можно удалить, но программа обижается. TODO
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button2_Click(object sender, EventArgs e)
        {
            AddValuesToCatalogLevelV1(new List<string[]>() { new string[] { "catalog" }, new string[] { "catalog_aggregate" }, new string[] { "catalog_model" } });
        }
    }
}
