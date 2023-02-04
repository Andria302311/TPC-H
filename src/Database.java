//This assignment allows you to apply a variety of basic concepts you have learnt in the course of this semester.
//
//TPC-H is one of the most important benchmarks in the area of databases. All large database systems try to optimize their performance for this benchmark. In order to compare how efficient a database processes queries, it makes sense to program queries by hand in order to determine what a database system might achieve if it incurred no overhead.
//
//The goal of this assignment is to read in three files, parse their contents and join them in order to answer simple queries to the given data set as efficiently as possible.
//
//The queries which your program is meant to answer are as follows. What is average quantity of line items of a customer's order belonging to a particular market segment. The signature of the corresponding method is given by public long getAverageQuantityPerMarketSegment(String marketsegment).
//
//Subsequently, your approach is detailed so that no further database background is required.

import javax.sound.sampled.Line;
import java.awt.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//The template provides you with classes Customer, LineItem and Order representign the corresponding database tables. The class Database with your implementation has already the static attribute private static Path baseDataDirectory together with a setter method to change its value for tests.
//
//Your first task is to read in the three included *.tbl files form the baseDataDirectory. Your implemented methods should look as follows:
//
//public static Stream<LineItem> processInputFileLineItem(), public static Stream<Customer> processInputFileCustomer() and public static Stream<Order> processInputFileOrder(). .tbl files are .csv files which instead of , use | for separating values.
//
//The Quantity of Lineitems you should parse with Integer.parseInt(str) * 100 in order to subsequently operate on int values only.
public class Database {
    private static Path baseDataDirectory = Paths.get("data");

    public static void setBaseDataDirectory(Path baseDataDirectory) {
        Database.baseDataDirectory = baseDataDirectory;
    }

    public static Stream<Customer> processInputFileCustomer() {
        try {
            Stream<String> files = Files.readAllLines(Paths.get(baseDataDirectory.toString()+ "/customer.tbl")).stream();
            return files.map(Map1);
        } catch (IOException e) {
            System.out.println("Data path seems to be wrong");
        }
        return null;
    }

    private static Function<String, Customer> Map1 = (line) -> {
        String[] cust = line.split("[|]");
        Customer customer1 =  new Customer(Integer.parseInt(cust[1].substring(9)), cust[2].toCharArray(), Integer.parseInt(cust[3]),
                cust[4].toCharArray(), Float.parseFloat(cust[5]), cust[6], cust[7].toCharArray());
        return customer1;
    };

    public static Stream<LineItem> processInputFileLineItem() {
        try {
            return Files.readAllLines(Paths.get(baseDataDirectory.toString() + "/lineitem.tbl")).stream().map(Map2);
        } catch (IOException e) {
            System.out.println("Data path seems to be wrong");
        }
        return null;
    }
    private static Function<String, LineItem> Map2 = (lineitem) -> {
        String[] line = lineitem.split("[|]");

        LocalDate localDate = LocalDate.parse(line[10]);
        LocalDate localDate1 = LocalDate.parse(line[11]);
        LocalDate localDate2 = LocalDate.parse(line[12]);

        return new LineItem(Integer.parseInt(line[0]), Integer.parseInt(line[1]),
                Integer.parseInt(line[2]), Integer.parseInt(line[3]),
                Integer.parseInt(line[4]) * 100, Float.parseFloat(line[5]), Float.parseFloat(line[6]),
                Float.parseFloat(line[7]), line[8].charAt(0), line[9].charAt(0), localDate, localDate1, localDate2, line[13].toCharArray(), line[14].toCharArray(), line[14].toCharArray());
    };

    public static Stream<Order> processInputFileOrders() {
        try {
            return Files.readAllLines(Paths.get(baseDataDirectory.toString() + "/orders.tbl")).stream().map(Map3);
        } catch (IOException e) {
            System.out.println("Data path seems top be wrong");
        }
        return null;
    }
    private static Function<String, Order> Map3 = (line) -> {
        String[] order = line.split("[|]");
        LocalDate localDate = LocalDate.parse(order[4]);

        return  new Order(Integer.parseInt(order[0]), Integer.parseInt(order[1]), order[2].charAt(0),
                Float.parseFloat(order[3]), localDate,  order[5].toCharArray(),  order[6].toCharArray(), Integer.parseInt(order[7]),  order[8].toCharArray());
    };
    public long getAverageQuantityPerMarketSegment(String marketsegment) {
        Map<Integer, String> map1 = new HashMap<>();
        processInputFileCustomer().forEach(n -> map1.put(n.custKey, n.mktsegment));
        Map<Integer, String> map2 = new HashMap<>();
        processInputFileOrders().forEach(n -> map2.put(n.orderKey, map1.get(n.custKey)));
        long quantity = processInputFileLineItem().filter(item -> marketsegment.equals(map2.get(item.orderKey)))
                .mapToLong(item -> item.quantity).sum();
        long orders = (long) processInputFileLineItem().filter(item -> marketsegment.equals(map2.get(item.orderKey))).count();
        if (orders == 0) {
            return 0;
        } else {
            return quantity / orders;
        }
    }

    public static void main(String[] args) {
        Database database = new Database();
        System.out.println(database.getAverageQuantityPerMarketSegment("BUILDING"));
    }
}
//In order to realize Joins of tables, you should use the classes of the interface Map in Java. First, you should map for each Customer, custKey -> marketSegment. Then, you should map for each Order, orderkey -> marketSegment by using custkey. Now, you should use this second Map for iterating over all LineItems in order to determine to which marketSegment it belongs.
// Implement the method public long getAverageQuantityPerMarketSegment(String marketsegment) of the class Database which as parameter receives a String marketsegment and returnd the average quantity of LineItem per order in marketsegment. During computation of the average, store the number as well as the total sum as long. Only at the very end, compute the quotient of these two values and return it again as long value (integer division). You are permitted to introduce further auxiliary methods for this class.