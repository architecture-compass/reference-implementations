package com.architecturecompass.rag;

import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Builds the connection pool from DATABASE_URL (postgres://user:pass@host:port/db). */
@Configuration
class DatabaseConfig {

    @Bean
    DataSource dataSource() {
        URI uri = URI.create(System.getenv("DATABASE_URL"));
        String[] credentials = uri.getUserInfo().split(":", 2);
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl("jdbc:postgresql://" + uri.getHost() + ":" + uri.getPort() + uri.getPath());
        ds.setUsername(credentials[0]);
        ds.setPassword(credentials[1]);
        ds.setMaximumPoolSize(5);
        return ds;
    }
}
