//package in.novopay.spark_poc;
//
//import in.novopay.spark_poc.jobs.AlmActiveJob;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Component;
//
//@Component
//public class JobScheduler {
//
//    @Autowired
//    private AlmActiveJob almActiveJob;
//
//    @Scheduled(cron = "0 0 * * * ?")
//    public void scheduleJob() {
//        almActiveJob.loadData();
//    }
//}
//
