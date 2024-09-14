package org.kafka.project;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.kafka.project.model.Person;
import org.kafka.project.model.RandomPeopleData;

import java.io.FileReader;

@Slf4j
public class DataLoader {
    public static ObjectMapper mapper = new ObjectMapper();
    public static String fileName = "src/main/resources/random-people-data.json";

    public static Person[] getPeople() {
        try {
            RandomPeopleData data = mapper.readValue(new FileReader(fileName), RandomPeopleData.class);
            return data.getCtRoot();
        } catch (Exception e) {
            log.error(e.toString());
            return new Person[0];
        }

    }
}
