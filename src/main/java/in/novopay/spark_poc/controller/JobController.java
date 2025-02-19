package in.novopay.spark_poc.controller;

import in.novopay.spark_poc.jobs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private static final Logger logger = LoggerFactory.getLogger(JobController.class);

    private final AlmActiveJob almActiveJob;
    private final LoanAccountFlattenedEntity loanAccountFlattenedEntity;
    private final LoanAccountFlattenedEntityNew loanAccountFlattenedEntityNew;
    private final AlmActiveSparkJobTest almActiveSparkJobTest;
    private final TestLeftOuterJoin testLeftOuterJoin;
//    private final AlmActiveKafkaToClickHouse almActiveKafkaToClickHouse;

    @Autowired
    public JobController(AlmActiveJob almActiveJob,
                         LoanAccountFlattenedEntity loanAccountFlattenedEntity,
                         LoanAccountFlattenedEntityNew loanAccountFlattenedEntityNew,
                         AlmActiveSparkJobTest almActiveSparkJobTest,TestLeftOuterJoin testLeftOuterJoin) {
        this.almActiveJob = almActiveJob;
        this.loanAccountFlattenedEntity = loanAccountFlattenedEntity;
        this.loanAccountFlattenedEntityNew = loanAccountFlattenedEntityNew;
        this.almActiveSparkJobTest = almActiveSparkJobTest;
//        this.almActiveKafkaToClickHouse = almActiveKafkaToClickHouse;
        this.testLeftOuterJoin = testLeftOuterJoin;
    }

    @GetMapping("/run-almActive-job")
    public ResponseEntity<String> runAlmActiveJob() {
        logger.info("Starting ALM Active job...");
        try {
            almActiveJob.loadData();
            logger.info("ALM Active job completed successfully.");
            return ResponseEntity.ok("ALM Active job executed successfully!");
        } catch (Exception e) {
            logger.error("Error executing ALM Active job: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error executing ALM Active job: " + e.getMessage());
        }
    }

    @GetMapping("/run-loan-account-job")
    public ResponseEntity<String> runLoanAccountJob() {
        logger.info("Starting Loan Account job...");
        try {
            loanAccountFlattenedEntity.loadData();
            logger.info("Loan Account job completed successfully.");
            return ResponseEntity.ok("Loan Account job executed successfully!");
        } catch (Exception e) {
            logger.error("Error executing Loan Account job: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error executing Loan Account job: " + e.getMessage());
        }
    }

    @GetMapping("/run-loan-account-new-job")
    public ResponseEntity<String> runLoanAccountNewJob() {
        logger.info("Starting Loan Account New job...");
        try {
            loanAccountFlattenedEntityNew.loadData();
            logger.info("Loan Account New job completed successfully.");
            return ResponseEntity.ok("Loan Account New job executed successfully!");
        } catch (Exception e) {
            logger.error("Error executing Loan Account New job: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error executing Loan Account New job: " + e.getMessage());
        }
    }

//    @GetMapping("/run-almActive-spark-job")
//    public ResponseEntity<String> runAlmActiveSparkJob() {
//        logger.info("Starting ALM Active Spark job...");
//        try {
//            almActiveSparkJob.loadData();
//            logger.info("ALM Active Spark job completed successfully.");
//            return ResponseEntity.ok("ALM Active Spark job executed successfully!");
//        } catch (Exception e) {
//            logger.error("Error executing ALM Active Spark job: {}", e.getMessage(), e);
//            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
//                    .body("Error executing ALM Active Spark job: " + e.getMessage());
//        }
//    }

  /*  @GetMapping("/run-almActive-clickhouse-job")
    public ResponseEntity<String> almActiveKafkaToClickHouse() {
        logger.info("Starting ALM Active Spark job...");
        try {
            almActiveKafkaToClickHouse.loadData();
            logger.info("ALM Active Spark job completed successfully.");
            return ResponseEntity.ok("ALM Active Spark job executed successfully!");
        } catch (Exception e) {
            logger.error("Error executing ALM Active Spark job: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error executing ALM Active Spark job: " + e.getMessage());
        }
    }*/

    @GetMapping("/run-almActive-spark-job")
    public ResponseEntity<String> runAlmActiveSparkJob() {
        logger.info("Starting ALM Active Spark job...");
        try {
            almActiveSparkJobTest.loadData();
            logger.info("ALM Active Spark job completed successfully.");
            return ResponseEntity.ok("ALM Active Spark job executed successfully!");
        } catch (Exception e) {
            logger.error("Error executing ALM Active Spark job: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error executing ALM Active Spark job: " + e.getMessage());
        }
    }


    @GetMapping("/test-left-outer-join")
    public ResponseEntity<String> testLeftOuterJoin() {
        logger.info("Starting ALM Active Spark job...");
        try {
            testLeftOuterJoin.loadData();
            logger.info("ALM Active Spark job completed successfully.");
            return ResponseEntity.ok("ALM Active Spark job executed successfully!");
        } catch (Exception e) {
            logger.error("Error executing ALM Active Spark job: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error executing ALM Active Spark job: " + e.getMessage());
        }
    }






}
