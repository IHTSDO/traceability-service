package org.ihtsdo.otf.traceabilityservice.configuration.jms;

import jakarta.jms.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;

@Configuration
public class JmsConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(JmsConfiguration.class);

    @Bean
    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory(ConnectionFactory connectionFactory, DefaultJmsListenerContainerFactoryConfigurer configurer) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setErrorHandler(t -> LOGGER.error("Failed to consume message.", t));
        configurer.configure(factory, connectionFactory);
        return factory;
    }
}
