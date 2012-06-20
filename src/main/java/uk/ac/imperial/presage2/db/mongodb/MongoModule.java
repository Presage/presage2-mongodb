/**
 * 	Copyright (C) 2011 Sam Macbeth <sm1106 [at] imperial [dot] ac [dot] uk>
 *
 * 	This file is part of Presage2.
 *
 *     Presage2 is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     Presage2 is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser Public License
 *     along with Presage2.  If not, see <http://www.gnu.org/licenses/>.
 */
package uk.ac.imperial.presage2.db.mongodb;

import java.util.Properties;

import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.mongodb.DB;

import uk.ac.imperial.presage2.core.db.DatabaseModule;
import uk.ac.imperial.presage2.core.db.DatabaseService;
import uk.ac.imperial.presage2.core.db.StorageService;

public class MongoModule extends DatabaseModule {

	final String host;
	final String username;
	final String password;
	final String db;

	public MongoModule() {
		super();
		this.host = "127.0.0.1";
		this.username = "";
		this.password = "";
		this.db = "presage";
	}

	public MongoModule(Properties props) {
		super();
		host = props.getProperty("mongo.host", "127.0.0.1");
		username = props.getProperty("mongo.user", "");
		password = props.getProperty("mongo.pass", "");
		db = props.getProperty("mongo.db", "presage");
	}

	@Override
	protected void configure() {
		bind(String.class).annotatedWith(Names.named("mongo.host")).toInstance(
				host);
		bind(String.class).annotatedWith(Names.named("mongo.db"))
				.toInstance(db);
		if (username != "") {
			bind(String.class).annotatedWith(Names.named("mongo.user"))
					.toInstance(username);
			bind(String.class).annotatedWith(Names.named("mongo.pass"))
					.toInstance(password);
		}

		bind(MongoStorage.class).in(Singleton.class);
		bind(DatabaseService.class).to(MongoStorage.class);
		bind(StorageService.class).to(MongoStorage.class);
		bind(DB.class).toProvider(MongoStorage.class);
	}

}
