package dev.tolstov.SG_Jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
public class SgJdbcApplication implements CommandLineRunner {
	private static final Logger log = LoggerFactory.getLogger(SgJdbcApplication.class);

	@Autowired
	JdbcTemplate jdbcTemplate;



	@Override
	public void run(String... args) throws Exception {
		log.info("creating tables");

		jdbcTemplate.execute("DROP TABLE customers IF EXISTS");
		jdbcTemplate.execute("CREATE TABLE customers(" +
				"id SERIAL, first_name VARCHAR(255), last_name VARCHAR(255))");

		List<Object[]> customerList = Stream.of(
						"Ivan Ivanoff",
						"Maria Grovoma",
						"Oleg Amperov"
				)
				.map(name -> name.split(" "))
				.collect(Collectors.toList());

		jdbcTemplate.batchUpdate("INSERT INTO customers(first_name, last_name)" +
				"VALUES(?,?)", customerList);

		log.info("Query for customer with name Oleg");
		jdbcTemplate.query(
				"SELECT id, first_name, last_name " +
				"FROM customers " +
				"WHERE first_name=?",
				((rs, rowNum) ->
						new Customer(rs.getLong("id"),
						rs.getString("first_name"),
						rs.getString("last_name"))
				),
				new Object[] { "Oleg" }
		).forEach(customer -> log.info(customer.toString()));


	}

	public static void main(String[] args) {
		SpringApplication.run(SgJdbcApplication.class, args);
	}
}
