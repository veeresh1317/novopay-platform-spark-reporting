package in.novopay.spark_poc.controller;

import in.novopay.spark_poc.jobs.AlmActiveJob;
//import in.novopay.spark_poc.jobs.RBIADFAccountDetails;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/jobs")
public class JobController {

    @Autowired
    private AlmActiveJob almActiveJob;

    //@Autowired
//    private RBIADFAccountDetails rbiADFAccountDetails;

    @GetMapping("/run-almActive-job")
    public String runAlmActiveJob() {
        almActiveJob.loadData();
        return "ALM Active job executed successfully!";
    }

//    @GetMapping("/run-rbi-adf-account-job")
//    public String runRbiAdfJob() {
//        rbiADFAccountDetails.loadData();
//        return "ALM Active job executed successfully!";
//    }
}

