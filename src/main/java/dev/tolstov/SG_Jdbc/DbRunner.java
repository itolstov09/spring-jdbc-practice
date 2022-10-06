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
        jdbcTemplate.execute("DROP TABLE customers IF EXISTS");
        jdbcTemplate.execute("CREATE TABLE customers(" +
                "id SERIAL, first_name VARCHAR(255), last_name VARCHAR(255))");

        List<Object[]> customerList = Stream.of(
                        "Ivan Ivanoff",
                        "Maria Grovoma",
                        "Oleg Amperov",
                        "Oleg Gazmanov",
                        "Vasiliy Gazmanov"
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
        String customerFirstName = findFirstNameByID(customerId);
        log.info(
                String.format("Customer  with id %d first name is: %s",
                        customerId,
                        customerFirstName) );

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
