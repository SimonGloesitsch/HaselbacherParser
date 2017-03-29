using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Data;
using System.Globalization;

namespace Database_Parser
{
    class Program
    {
        static void Main(string[] args)
        {
            MySqlDataReader reader;

            //Tables von der HaselbacherDB
            DataTable gebaeudeHaselbacher = new DataTable();
            DataTable messungFernwaermeVentilHaselbacher = new DataTable();
            DataTable messungBoilerHaselbacher = new DataTable();
            DataTable messungHeizKreisHaselbacher = new DataTable();
            DataTable messungStromZaehlerHaselbacher = new DataTable();
            DataTable messungTransportPumpeHaselbacher = new DataTable();
            DataTable messungWaermeZaehlerHaselbacher = new DataTable();
            DataTable messungReglerHaselbacher = new DataTable();

            //Connection
            string connectionStringHaselbacher;
            connectionStringHaselbacher = "SERVER=127.0.0.1;DATABASE=haselbacher;USER=root;PASSWORD=;Convert Zero Datetime=True";
            MySqlConnection connection = new MySqlConnection(connectionStringHaselbacher);

            string connectionStringEnergieatlas;
            connectionStringEnergieatlas = "SERVER=127.0.0.1;DATABASE=energieatlas;USER=root;PASSWORD=;Convert Zero Datetime=True";

            try
            {
                connection.Open();

                //SQL Query
                MySqlCommand command = new MySqlCommand();
                command.Connection = connection;

                string commandString;

                //Table für Gebäude
                commandString = "SELECT gebaeudeID, gebaeudetypID, strasse, plz, katastralgemeinde FROM gebaeude";
                command.CommandText = commandString;
                reader = command.ExecuteReader();

                gebaeudeHaselbacher.Load(reader);

                //foreach (DataRow row in gebaeudeHaselbacher.Rows)
                //{
                //    Console.WriteLine(row["gebaeudeID"].ToString());
                //    Console.WriteLine(row["gebaeudetypID"].ToString());
                //    Console.WriteLine(row["strasse"].ToString());
                //    Console.WriteLine(row["plz"].ToString());
                //    Console.WriteLine(row["katastralgemeinde"].ToString());
                //}

                //Table für Messung (Fernwaermeventil)
                commandString = "SELECT r.GebaeudeID, messung.VorlauftemperaturPrimaer, messung.VorlaufIstTemperaturSekundaer, messung.RuecklaufIstTemperaturPrimaer, messung.Messzeitpunkt FROM messung_fernwaermeventil AS messung LEFT JOIN fernwaermeventil AS f ON (messung.FernwaermeventilID = f.FernwaermeVentilID) LEFT JOIN regler AS r ON (f.ReglerID = r.ReglerID)";
                command.CommandText = commandString;
                reader = command.ExecuteReader();

                messungFernwaermeVentilHaselbacher.Load(reader);

                //foreach (DataRow row in messungFernwaermeVentilHaselbacher.Rows)
                //{
                //    Console.WriteLine(row["GebaeudeID"].ToString());
                //    Console.WriteLine(row["VorlauftemperaturPrimaer"].ToString());
                //    Console.WriteLine(row["VorlaufIstTemperaturSekundaer"].ToString());
                //    Console.WriteLine(row["RuecklaufIstTemperaturPrimaer"].ToString());
                //    Console.WriteLine(row["Messzeitpunkt"].ToString());
                //}

                //Table für Messung (Boiler)
                commandString = "SELECT r.GebaeudeID, messung.Isttemperatur, messung.Vorlauftemperatur, messung.Ruecklauftemperatur, messung.MessZeitpunkt FROM messung_boiler AS messung LEFT JOIN boiler AS b ON (messung.boilerID = b.boilerID) LEFT JOIN regler AS r ON (b.ReglerID = r.ReglerID)";
                command.CommandText = commandString;
                reader = command.ExecuteReader();

                messungBoilerHaselbacher.Load(reader);

                //foreach (DataRow row in messungBoilerHaselbacher.Rows)
                //{
                //    Console.WriteLine(row["GebaeudeID"].ToString());
                //    Console.WriteLine(row["Isttemperatur"].ToString());
                //    Console.WriteLine(row["Vorlauftemperatur"].ToString());
                //    Console.WriteLine(row["Ruecklauftemperatur"].ToString());
                //    Console.WriteLine(row["Messzeitpunkt"].ToString());
                //}

                //Table für Messung (Heizkreis)
                commandString = "SELECT r.GebaeudeID, messung.Vorlaufisttemperatur, messung.Ruecklaufisttemperatur, messung.Raumsollisttemperatur, messung.Messzeitpunkt FROM messung_heizkreis AS messung LEFT JOIN heizkreis AS h ON (messung.HeizkreisID = h.HeizkreisID) LEFT JOIN regler AS r ON (h.reglerID = r.reglerID)";
                command.CommandText = commandString;
                reader = command.ExecuteReader();

                messungHeizKreisHaselbacher.Load(reader);

                //foreach (DataRow row in messungHeizKreisHaselbacher.Rows)
                //{
                //    Console.WriteLine(row["GebaeudeID"].ToString());
                //    Console.WriteLine(row["Vorlaufisttemperatur"].ToString());
                //    Console.WriteLine(row["Ruecklaufisttemperatur"].ToString());
                //    Console.WriteLine(row["Raumsollisttemperatur"].ToString());
                //    Console.WriteLine(row["Messzeitpunkt"].ToString());
                //}

                //Table für Messung (Regler)
                commandString = "SELECT r.GebaeudeID, messung.ReglerTemperatur, messung.Aussentemperatur, messung.Messzeitpunkt FROM messung_regler AS messung LEFT JOIN regler AS r ON (messung.ReglerID = r.ReglerID)";
                command.CommandText = commandString;
                reader = command.ExecuteReader();

                messungReglerHaselbacher.Load(reader);

                //foreach (DataRow row in messungReglerHaselbacher.Rows)
                //{
                //    Console.WriteLine(row["GebaeudeID"].ToString());
                //    Console.WriteLine(row["ReglerTemperatur"].ToString());
                //    Console.WriteLine(row["Aussentemperatur"].ToString());
                //    Console.WriteLine(row["Messzeitpunkt"].ToString());
                //}

                //Table für Messung (Stromzaehler)
                commandString = "SELECT r.GebaeudeID, messung.Leistung, messung.Energieverbrauch, messung.Messzeitpunkt FROM messung_stromzaehler AS messung LEFT JOIN stromzaehler AS s ON (messung.StromzaehlerID = s.StromzaehlerID) LEFT JOIN regler AS r ON (s.ReglerID = r.ReglerID)";
                command.CommandText = commandString;
                reader = command.ExecuteReader();

                messungStromZaehlerHaselbacher.Load(reader);

                //foreach (DataRow row in messungStromZaehlerHaselbacher.Rows)
                //{
                //    Console.WriteLine(row["GebaeudeID"].ToString());
                //    Console.WriteLine(row["Leistung"].ToString());
                //    Console.WriteLine(row["Energieverbrauch"].ToString());
                //    Console.WriteLine(row["Messzeitpunkt"].ToString());
                //}

                //Table für Messung (Transportpumpe)
                commandString = "SELECT r.GebaeudeID, messung.Drehzahl, messung.Messzeitpunkt FROM messung_transportpumpe AS messung LEFT JOIN transportpumpe AS t ON (messung.TransportpumpeID = t.TransportpumpeID) LEFT JOIN regler AS r ON (t.ReglerID = r.ReglerID)";
                command.CommandText = commandString;
                reader = command.ExecuteReader();

                messungTransportPumpeHaselbacher.Load(reader);

                //foreach (DataRow row in messungTransportPumpeHaselbacher.Rows)
                //{
                //    Console.WriteLine(row["GebaeudeID"].ToString());
                //    Console.WriteLine(row["Drehzahl"].ToString());
                //    Console.WriteLine(row["Messzeitpunkt"].ToString());
                //}

                //Table für Messung (Waermezaehler)
                commandString = "SELECT r.GebaeudeID, messung.EnergieVerbrauch, messung.Leistung, messung.Messzeitpunkt FROM messung_waermezaehler AS messung LEFT JOIN waermezaehler AS w ON (messung.WaermezaehlerID = w.WaermezaehlerID) LEFT JOIN regler AS r ON (r.ReglerID = r.ReglerID)";
                command.CommandText = commandString;
                reader = command.ExecuteReader();

                messungWaermeZaehlerHaselbacher.Load(reader);

                //foreach (DataRow row in messungWaermeZaehlerHaselbacher.Rows)
                //{
                //    Console.WriteLine(row["GebaeudeID"].ToString());
                //    Console.WriteLine(row["Energieverbrauch"].ToString());
                //    Console.WriteLine(row["Leistung"].ToString());
                //    Console.WriteLine(row["Messzeitpunkt"].ToString());
                //}

                connection.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            connection = new MySqlConnection(connectionStringEnergieatlas);

            try
            {
                connection.Open();

                MySqlCommand command = new MySqlCommand();
                command.Connection = connection;

                string commandString;
                string messzeitpunkt;

                DateTime time;

                foreach (DataRow row in gebaeudeHaselbacher.Rows)
                {
                    commandString = "INSERT INTO gebaeude VALUES ('" + Int32.Parse(row["gebaeudeID"].ToString()) + "','" + Int32.Parse(row["gebaeudetypID"].ToString()) + "','0000-00-00 00:00:00','" + row["strasse"].ToString() + "','" + row["plz"].ToString() + "','" + row["katastralgemeinde"].ToString() + "','0','0');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();
                }

                //      Kategorien
                //  1.) Temperatur
                //  2.) VorlaufTemperatur
                //  3.) RuecklaufTemperatur
                //  4.) VorlaufTemperaturPrimaer
                //  5.) VorlaufTemperaturSekundaer
                //  6.) IstTemperatur
                //  7.) RaumTemperatur
                //  8.) AussenTemperatur
                //  9.) Leistung
                // 10.) Energieverbrauch
                // 11.) Drehzahl

                //      Messungsarten
                //  1.) Boiler           - IstTemperatur
                //  2.) Boiler           - VorlaufTemperatur
                //  3.) Boiler           - RuecklaufTemperatur
                //  4.) FernwaermeVentil - VorlaufTemperaturPrimaer
                //  5.) FernwaarmeVentil - VorlaufTemperaturSekundaer
                //  6.) FernwaermeVenitl - RuecklaufTemperatur
                //  7.) Heizkreis        - VorlaufTemperatur
                //  8.) Heizkreis        - RuecklaufTemperatur
                //  9.) Heizkreis        - RaumTemperatur
                // 10.) StromZaehler     - Leistung
                // 11.) StromZaehler     - Energieverbrauch
                // 12.) TransportPumpe   - Drehzahl
                // 13.) WaermeZaehler    - Energieverbrauch
                // 14.) WaermeZaehler    - Leistung
                // 15.) Regler           - Temperatur
                // 16.) Regler           - AussenTemperatur

                foreach (DataRow row in messungBoilerHaselbacher.Rows)
                {
                    time = DateTime.Parse(row["Messzeitpunkt"].ToString());

                    messzeitpunkt = time.Year + "-" + time.Month + "-" + time.Day + " " + time.TimeOfDay;

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','1','" + float.Parse(row["Isttemperatur"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','2','" + float.Parse(row["Vorlauftemperatur"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','3','" + float.Parse(row["Ruecklauftemperatur"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();
                }

                foreach (DataRow row in messungFernwaermeVentilHaselbacher.Rows)
                {
                    time = DateTime.Parse(row["Messzeitpunkt"].ToString());

                    messzeitpunkt = time.Year + "-" + time.Month + "-" + time.Day + " " + time.TimeOfDay;

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','4','" + float.Parse(row["VorlauftemperaturPrimaer"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','5','" + float.Parse(row["VorlaufIstTemperaturSekundaer"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','6','" + float.Parse(row["RuecklaufIstTemperaturPrimaer"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();
                }

                foreach (DataRow row in messungHeizKreisHaselbacher.Rows)
                {
                    time = DateTime.Parse(row["Messzeitpunkt"].ToString());

                    messzeitpunkt = time.Year + "-" + time.Month + "-" + time.Day + " " + time.TimeOfDay;

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','7','" + float.Parse(row["Vorlaufisttemperatur"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','8','" + float.Parse(row["Ruecklaufisttemperatur"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','9','" + float.Parse(row["Raumsollisttemperatur"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();
                }

                foreach (DataRow row in messungStromZaehlerHaselbacher.Rows)
                {
                    time = DateTime.Parse(row["Messzeitpunkt"].ToString());

                    messzeitpunkt = time.Year + "-" + time.Month + "-" + time.Day + " " + time.TimeOfDay;

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','10','" + float.Parse(row["Leistung"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','11','" + float.Parse(row["Energieverbrauch"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();
                }

                foreach (DataRow row in messungTransportPumpeHaselbacher.Rows)
                {
                    time = DateTime.Parse(row["Messzeitpunkt"].ToString());

                    messzeitpunkt = time.Year + "-" + time.Month + "-" + time.Day + " " + time.TimeOfDay;

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','12','" + float.Parse(row["Drehzahl"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();
                }

                foreach (DataRow row in messungWaermeZaehlerHaselbacher.Rows)
                {
                    time = DateTime.Parse(row["Messzeitpunkt"].ToString());

                    messzeitpunkt = time.Year + "-" + time.Month + "-" + time.Day + " " + time.TimeOfDay;

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','13','" + float.Parse(row["Energieverbrauch"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','14','" + float.Parse(row["Leistung"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();
                }

                foreach (DataRow row in messungReglerHaselbacher.Rows)
                {
                    time = DateTime.Parse(row["Messzeitpunkt"].ToString());

                    messzeitpunkt = time.Year + "-" + time.Month + "-" + time.Day + " " + time.TimeOfDay;

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','15','" + float.Parse(row["ReglerTemperatur"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();

                    commandString = "INSERT INTO `messung` VALUES ('" + Int32.Parse(row["GebaeudeID"].ToString()) + "','16','" + float.Parse(row["Aussentemperatur"].ToString()) + "','" + messzeitpunkt + "');";
                    command.CommandText = commandString;
                    command.ExecuteNonQuery();
                }

                connection.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }
    }
}
