package com.example.demo.service;

import java.util.List;

import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.example.demo.entity.User;
import com.example.demo.repo.UserRepo;

@Service	
public class UserService {

	@Autowired UserRepo userRepo;

	public void saveAll(List<? extends User> items) {
		items.stream().forEach(v -> {
			userRepo.save(v);
		});
	}
}