package org.kafka.project.model;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Getter
@NoArgsConstructor
public class Person implements Serializable {
    private String _id;
    private String name;
    private String dob;
    private Address address;
    private String telephone;
    private String[] pets;
    private Double score;
    private String email;
    private String url;
    private String description;
    private Boolean verified;
    private Double salary;

    @Getter
    @NoArgsConstructor
    public static class Address implements Serializable {
        private String street;
        private String town;
        private String postode;
    }
}
