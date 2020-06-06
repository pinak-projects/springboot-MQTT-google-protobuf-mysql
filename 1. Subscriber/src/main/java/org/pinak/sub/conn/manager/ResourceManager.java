package org.pinak.sub.conn.manager;

import java.util.ResourceBundle;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public final class ResourceManager {

	private ResourceManager() {
	}

	public static String getQueryValue(String key) {
		ResourceBundle bundle = ResourceBundle.getBundle("query");
		return bundle.getString(key);
	}

}
