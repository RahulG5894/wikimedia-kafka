package com.learning.springboot;

import com.learning.springboot.model.WikimediaData;
import com.learning.springboot.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);
    private WikimediaDataRepository wikimediaDataRepository;

    public KafkaDatabaseConsumer(WikimediaDataRepository wikimediaDataRepository) {
        this.wikimediaDataRepository = wikimediaDataRepository;
    }

    @KafkaListener(topics = "wikimedia_recent_change", groupId = "myGroup")
    public void consume(String eventMessage) {
        LOGGER.info("Message received -> {}", eventMessage);
        WikimediaData wikiData = new WikimediaData();
        wikiData.setWikiEventData(eventMessage);
        wikimediaDataRepository.save(wikiData);
    }
}
