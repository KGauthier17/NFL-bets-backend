// src/main/java/com/example/nfl_bets_backend/repository/PlayerRepository.java
package com.example.NFL_bets_backend.repository;

import com.example.NFL_bets_backend.model.Player;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PlayerRepository extends JpaRepository<Player, Long> {
}