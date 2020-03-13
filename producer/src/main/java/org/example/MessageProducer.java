package org.example;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.db.EmpSqlSessionFactory;
import org.example.mapper.EmployeeMapper;
import org.example.model.Employee;
import org.example.util.ExtLimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class MessageProducer {
    private static Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    private KafkaProducer<Integer, Employee> kafkaProducer;
    SqlSessionFactory sqlSessionFactory;

    public MessageProducer(Properties properties) {
        kafkaProducer = new KafkaProducer<Integer, Employee>(properties);
    }

    public void init() throws IOException {
        InputStream inputStream = Resources.getResourceAsStream("mybatis-config.xml");
        sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
//        sqlSessionFactory = EmpSqlSessionFactory.build();
    }

    public void send() {
        try (SqlSession sqlSession = sqlSessionFactory.openSession()) {
//            Employee employee = sqlSession.selectOne("org.example.mapper.EmployeeMapper", 10036);
            EmployeeMapper employeeMapper = sqlSession.getMapper(EmployeeMapper.class);
            Employee employee = employeeMapper.selectById(Long.valueOf(10036L));
            logger.info(employee.toString());
            logger.info("################ org.apache.kafka.clients.producer.KafkaProducer.initTransactions ################");
            kafkaProducer.initTransactions();
            logger.info("################ org.apache.kafka.clients.producer.KafkaProducer.beginTransaction  ################");
            kafkaProducer.beginTransaction();

            try {
                ProducerRecord<Integer, Employee> record = new ProducerRecord<Integer, Employee>("string-topic", employee.getEmployeeNo(), employee);
                logger.info("################## org.apache.kafka.clients.producer.KafkaProducer.send(ProducerRecord<K,V>) #####################");
                kafkaProducer.send(record);
//                kafkaProducer.sendOffsetsToTransaction();
            } catch (Exception ex) {
                kafkaProducer.abortTransaction();
            } finally {
                logger.info("############# org.apache.kafka.clients.producer.KafkaProducer.commitTransaction #################");
                kafkaProducer.commitTransaction();
                logger.info("end commit tx #############################################");
            }

        }
    }

    public void multiSend() {
        try (SqlSession sqlSession = sqlSessionFactory.openSession()) {
            EmployeeMapper employeeMapper = sqlSession.getMapper(EmployeeMapper.class);
            Employee ety = new Employee();
            ExtLimit limit = new ExtLimit();
            limit.setStart(101);
            limit.setLimit(1000);
            List<Employee> employees = employeeMapper.selectByLimit(ety, limit);
            logger.info("initTransactions ##################################");
            kafkaProducer.initTransactions();
            logger.info("beginTransaction #################################");
            kafkaProducer.beginTransaction();

            logger.info("start send #######################################");
            try {
                for (Employee employee : employees) {
                    ProducerRecord<Integer, Employee> record = new ProducerRecord<Integer, Employee>("string-topic", employee.getEmployeeNo(), employee);
                    kafkaProducer.send(record);
                }

            } catch (Exception ex) {
                logger.info(ex.getMessage());
                kafkaProducer.abortTransaction();
            } finally {
                logger.info("start commit tx #############################################");
                kafkaProducer.commitTransaction();
                logger.info("end commit tx #############################################");
            }

        }
    }

    public void stop() {
        kafkaProducer.close();
    }
}
