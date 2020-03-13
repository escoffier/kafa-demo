package org.example.db;

import org.apache.ibatis.datasource.DataSourceFactory;
import org.apache.ibatis.datasource.pooled.PooledDataSource;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.example.mapper.EmployeeMapper;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;

public class EmpSqlSessionFactory {
    SqlSessionFactory sqlSessionFactory;
//    DataSource dataSource;

    public EmpSqlSessionFactory() {
        DataSource dataSource = new PooledDataSource(
                "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://localhost:3306/employees?useUnicode=true",
                "root",
                "19811981");

        TransactionFactory transactionFactory = new JdbcTransactionFactory();
        Environment environment = new Environment("development", transactionFactory, dataSource);
        Configuration configuration = new Configuration(environment);
        configuration.addMapper(EmployeeMapper.class);
        sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);
    }

    public static SqlSessionFactory build() {
        DataSource dataSource = new PooledDataSource(
                "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://localhost:3306/employees?useUnicode=true",
                "root",
                "19811981");

        TransactionFactory transactionFactory = new JdbcTransactionFactory();
        Environment environment = new Environment("development", transactionFactory, dataSource);
        Configuration configuration = new Configuration(environment);
        configuration.addMapper(EmployeeMapper.class);
        return new SqlSessionFactoryBuilder().build(configuration);
    }

    public boolean init() {
//        try {
//            InputStream inputStream = Resources.getResourceAsStream("mybatis-config.xml");
//            sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
//        } catch (IOException ex) {
//            return false;
//        }
        return true;
    }
}
