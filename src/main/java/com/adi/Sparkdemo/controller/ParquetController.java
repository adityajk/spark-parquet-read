package com.adi.Sparkdemo.controller;

import com.adi.Sparkdemo.service.ParquetService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/parquet")
public class ParquetController {

    @Autowired
    private ParquetService parquetService;

    @GetMapping("/join")
    public String joinParquetFiles(@RequestParam String joinColumn) {
        return parquetService.readAndJoinParquetFiles(joinColumn);
    }
}
