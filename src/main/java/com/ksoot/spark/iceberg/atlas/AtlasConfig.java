package com.ksoot.spark.iceberg.atlas;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;

@Configuration
public class AtlasConfig {

  public static final String ATLAS_PREFIX = "atlas.";
  public static final String ATLAS_PROPERTIES_BEAN_NAME = "atlasProperties";

  public static final String ATLAS_URL = "atlas.url";
  public static final String ATLAS_USERNAME_PROPERTY = "atlas.username";
  public static final String ATLAS_PASSWORD_PROPERTY = "atlas.password";

  @Bean
  AtlasClientV2 atlasClient(final Properties atlasProperties) throws AtlasException {
    String[] atlasServerUrls = atlasProperties.getProperty(ATLAS_URL, "").split(",");

    String[] basicAuthUsernamePassword = {
      atlasProperties.getProperty(ATLAS_USERNAME_PROPERTY),
      atlasProperties.getProperty(ATLAS_PASSWORD_PROPERTY)
    };

    basicAuthUsernamePassword = Arrays.stream(basicAuthUsernamePassword).filter(StringUtils::isNotBlank).toArray(String[]::new);

    org.apache.commons.configuration.Configuration atlasConfiguration =
        new MapConfiguration(atlasProperties);
    ApplicationProperties.set(atlasConfiguration);

    return new AtlasClientV2(atlasConfiguration, atlasServerUrls, basicAuthUsernamePassword);
  }

  @ConditionalOnMissingBean(name = ATLAS_PROPERTIES_BEAN_NAME)
  static class AtlasPropertiesConfiguration {

    @Bean
    Properties atlasProperties(final Environment environment) {
      if (environment instanceof ConfigurableEnvironment) {
        final List<PropertySource<?>> propertySources =
            ((ConfigurableEnvironment) environment)
                .getPropertySources().stream().collect(Collectors.toList());
        final List<String> sparkPropertyNames =
            propertySources.stream()
                .filter(propertySource -> propertySource instanceof EnumerablePropertySource)
                .map(propertySource -> (EnumerablePropertySource) propertySource)
                .map(EnumerablePropertySource::getPropertyNames)
                .flatMap(Arrays::stream)
                .distinct()
                .filter(key -> key.startsWith(ATLAS_PREFIX))
                .collect(Collectors.toList());

        return sparkPropertyNames.stream()
            .collect(
                Properties::new,
                (props, key) -> props.put(key, environment.getProperty(key)),
                Properties::putAll);
      } else {
        return new Properties();
      }
    }
  }
}
