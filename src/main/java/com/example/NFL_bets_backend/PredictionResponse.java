package com.example.NFL_bets_backend;

public class PredictionResponse {
    private double prediction;

    public PredictionResponse(double prediction) {
        this.prediction = prediction;
    }

    public double getPrediction() {
        return prediction;
    }

    public void setPrediction(double prediction) {
        this.prediction = prediction;
    }
}

