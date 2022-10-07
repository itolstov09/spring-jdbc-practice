package dev.tolstov.SG_Jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class DbRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DbRunner.class);

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Override
    public void run(String... args) {
        log.info("creating tables");
        jdbcTemplate.execute("DROP TABLE IF EXISTS customers");
        jdbcTemplate.execute("CREATE TABLE customers(" +
                "id SERIAL, first_name VARCHAR(255), last_name VARCHAR(255))");

        List<Object[]> customerList = Stream.of(
                        "Ivan Ivanoff",
                        "Maria Grovoma",
                        "Oleg Amperov",
                        "Oleg Gazmanov",
                        "Vasiliy Gazmanov",
                        "Viktor Amperov"
                )
                .map(name -> name.split(" "))
                .collect(Collectors.toList());

        int[] ints = insertFromList(customerList);
        log.info(String.format("ints: %s", Arrays.toString(ints)));


        log.info("Query for customers with name Oleg");
        List<Customer> olegs = findCustomersByFirstName("Oleg");
        olegs.forEach(oleg -> log.info(oleg.toString()));

        log.info("Query for customers with name Maria");
        List<Customer> marias = findCustomersByFirstName("Maria");
        marias.forEach(maria -> log.info(maria.toString()));

        int customerId = 1;
        String customerFirstNameWithId1 = findFirstNameByID(customerId);
        log.info(
                String.format("Customer  with id %d first name is: %s",
                        customerId,
                        customerFirstNameWithId1) );

        Integer customersCount = customersCount();
        log.info(String.format("Customers count is: %d", customersCount));

        Customer ivan = findCustomerByFirstName("Ivan");
        log.info(String.format("Customer with first name Ivan: %s", ivan.toString()));
//        В методе используется queryForObject, поэтому ожидается что результатом запроса будет 1 запись.
//        Вызовет исключение, так как в БД 2 записи с именем Oleg.
//        Customer oleg = findCustomerByFirstName("Oleg");

        List<Customer> gazmanovs = findCustomersByLastName("Gazmanov");
        log.info(String.format("Customers with last name Gazmanov: \"%s\"", gazmanovs));

        List<String> customersLastName = selectCustomersLastName();
        log.info(String.format("customersLastName: %s", customersLastName ));

        updateFirstNameById("Ilya", 250);

        updateFirstNameById("Mikhail", customerId);

        int updatedRowsCount = fixRepoInLastName("Amperov", "Amperoff");
        log.info(String.format("Fix repo in last_name \"Amperov\" to \"Amperoff\"\n %d rows updated", updatedRowsCount));

        customerId = 4;
        boolean userExists = isUserExists(customerId);
        log.info(String.format("Customer with id %d exists: %s", customerId, userExists));


        String customerFirstName = "Artem";
        String customerLastName = "Tarasov";
        insertCustomer(customerFirstName, customerLastName);


        List<Customer> customers = getCustomers();
        log.info("customers:");
        customers.forEach(customer -> log.info("\t" + customer.toString()));

        log.info(String.format("Delete customer with id %d", customerId));
        deleteById(customerId);

        List<Customer> customers1 = getCustomers();
        log.info("customers after deleting row:");
        customers1.forEach(customer -> log.info("\t" + customer.toString()));


    }

    private int fixRepoInLastName(String old, String new_) {
        return jdbcTemplate.update("UPDATE customers SET last_name=? WHERE last_name=?", new_, old);
    }

    private void deleteById(int customerId) {
        jdbcTemplate.update("DELETE FROM customers WHERE id=?", customerId);
    }

    private List<Customer> getCustomers() {
        return jdbcTemplate.query("SELECT * FROM customers", new CustomerRowMapper());
    }

    private void insertCustomer(String customerFirstName, String customerLastName) {
        jdbcTemplate.update("INSERT INTO customers(first_name, last_name)" +
                        "VALUES(?,?)",
                customerFirstName,
                customerLastName
        );
    }

    private void updateFirstNameById(String firstName, int customerId) {
        log.info(String.format("Update customer with id %d", customerId));
        int updatedRowsCount = jdbcTemplate.update(
                "UPDATE customers SET first_name=? WHERE id=?",
                firstName, customerId);
        log.info(String.format("Rows updated: %d", updatedRowsCount));
    }

    private boolean isUserExists(int customerId) {
        Integer customerCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM customers WHERE id=?",
                Integer.class,
                customerId
        );
        return customerCount > 0;
    }

    private List<String> selectCustomersLastName() {
        return jdbcTemplate.queryForList("SELECT last_name FROM customers",
                String.class);
    }

    private Integer customersCount() {
         return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM customers",
                 Integer.class);
    }

    private String findFirstNameByID(int customerId) {
        return  jdbcTemplate.queryForObject("SELECT first_name FROM customers WHERE id = ?",
                String.class,
                customerId );
    }




    //rowMapper - использование лямбд(Java 8)
    private List<Customer> findCustomersByFirstName(String firstName) {
        return jdbcTemplate.query(
                "SELECT id, first_name, last_name " +
                        "FROM customers " +
                        "WHERE first_name=?",
                ((rs, rowNum) ->
                        new Customer(rs.getLong("id"),
                                rs.getString("first_name"),
                                rs.getString("last_name"))
                ),
                firstName);
    }


    // свой класс rowMapper
    private List<Customer> findCustomersByLastName(String lastName) {
        return jdbcTemplate.query(
                "SELECT id, first_name, last_name " +
                        "FROM customers " +
                        "WHERE last_name=?",
                new CustomerRowMapper(),
                lastName);
    }

    // использование BeanPropertyRowMapper(удобная вещь)
    private Customer findCustomerByFirstName(String firstName) {
        return  jdbcTemplate.queryForObject("SELECT id, first_name, last_name FROM customers WHERE first_name = ?",
                new BeanPropertyRowMapper<>(Customer.class),
                firstName);
    }


    //возвращает массив содержащий количество обновлений в бд на каждый элемент списка (customerList)
    private int[] insertFromList(List<Object[]> customerList) {
        return jdbcTemplate.batchUpdate("INSERT INTO customers(first_name, last_name)" +
                "VALUES(?,?)", customerList);
    }
}
