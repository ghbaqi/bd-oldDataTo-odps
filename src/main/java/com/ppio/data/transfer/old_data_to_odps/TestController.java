package com.ppio.data.transfer.old_data_to_odps;

import org.springframework.boot.actuate.endpoint.invoke.convert.ConversionServiceParameterValueMapper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@RestController
public class TestController {

    private int i;

    @GetMapping("/hi")
    public String hi() {
        return "hi " + i++;
    }

    public static void main(String[] args) throws ParseException {
//        String curDateStr = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
//        System.out.println(curDateStr);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        System.out.println(dateFormat.parse("2020-12-11T00:05:00Z"));
    }
}
