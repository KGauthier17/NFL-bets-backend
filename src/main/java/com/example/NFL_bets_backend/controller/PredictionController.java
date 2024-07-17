// src/main/java/com/example/nflbets/controller/PredictionController.java
package com.example.NFL_bets_backend.controller;

import com.example.NFL_bets_backend.model.PredictionRequest;
import com.example.NFL_bets_backend.service.PredictionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/predictions")
public class PredictionController {

    @Autowired
    private PredictionService predictionService;

    @PostMapping
    public double getPrediction(@RequestBody PredictionRequest request) {
        return predictionService.predict(request);
    }
}
