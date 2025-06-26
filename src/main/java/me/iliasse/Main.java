package me.iliasse;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Main {
    public static void main(String[] args) {

        //creates a spark instance
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("IncidentsProcessing")
                .getOrCreate();

        //reads incidents.csv file
        Dataset<Row> df = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("incidents.csv");

        //maps file to a temporale View
        df.createOrReplaceTempView("incidents");

        //Displays number of incidents by service
        incidentsByService(sparkSession).show();

        //Displays top 2 years of incidents
        topYearsIncidents(sparkSession, 2).show();
    }

    private static Dataset<Row> incidentsByService(SparkSession parkSession){
        return parkSession.sql(
                "SELECT service, COUNT(*) AS nbr_incidents "+
                        "FROM incidents "+
                        "GROUP BY service"
        );
    }

    private static Dataset<Row> topYearsIncidents(SparkSession parkSession, int top){
        return parkSession.sql(
                "SELECT YEAR(TO_DATE(date)) AS year, COUNT(*) AS nbr_incidents " +
                        "FROM incidents " +
                        "GROUP BY year " +
                        "ORDER BY nbr_incidents DESC " +
                        "LIMIT " + top + ""
        );
    }
}