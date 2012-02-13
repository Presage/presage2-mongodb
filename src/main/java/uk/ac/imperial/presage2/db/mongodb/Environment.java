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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import uk.ac.imperial.presage2.core.db.persistent.PersistentEnvironment;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class Environment implements PersistentEnvironment {

	final static String envCollection = "environmentState";
	final static String transEnvCollection = "environmentStateTime";
	final DB db;
	final DBCollection env;
	final DBCollection transEnv;
	final DBObject state;
	final long simId;

	Environment(long simId, DB db) {
		super();
		this.db = db;
		this.simId = simId;

		env = db.getCollection(envCollection);
		transEnv = db.getCollection(transEnvCollection);

		env.ensureIndex(new BasicDBObject("simId", 1), "simId", true);

		DBObject index = new BasicDBObject();
		index.put("simId", 1);
		index.put("time", 1);
		transEnv.ensureIndex(index, "time", true);

		DBObject state = this.env.findOne(new BasicDBObject("simId", simId));

		if (state == null) {
			// create new
			this.state = new MongoObject();
			this.state.put("simId", simId);
			env.insert(this.state);
		} else {
			this.state = state;
		}
	}

	@Override
	public String getProperty(String key) {
		try {
			return this.state.get(key).toString();
		} catch (NullPointerException e) {
			return null;
		}
	}

	@Override
	public void setProperty(String key, String value) {
		this.state.put(key, value);
		this.env.save(this.state);
	}

	@Override
	public String getProperty(String key, int timestep) {
		try {
			return getState(timestep).get(key).toString();
		} catch (NullPointerException e) {
			return null;
		}
	}

	@Override
	public void setProperty(String key, int timestep, String value) {
		DBObject tState = getState(timestep);
		tState.put(key, value);
		transEnv.save(tState);
	}

	DBObject getState(int time) {
		DBObject query = new BasicDBObject();
		query.put("simId", simId);
		query.put("time", time);
		DBObject tState = transEnv.findOne(query);
		if (tState == null) {
			tState = new BasicDBObject();
			tState.put("simId", simId);
			tState.put("time", time);
			transEnv.save(tState);
		}
		return tState;
	}

	@Override
	public Map<String, String> getProperties() {
		Map<String, String> properties = new HashMap<String, String>();
		for (String key : this.state.keySet()) {
			// skip simId key
			if (key.equals("simId") || key.equals("_id")) {
				continue;
			}
			properties.put(key, this.state.get(key).toString());
		}
		return Collections.unmodifiableMap(properties);
	}

	@Override
	public Map<String, String> getProperties(int timestep) {
		Map<String, String> properties = new HashMap<String, String>();
		DBObject tState = this.getState(timestep);
		for (String key : tState.keySet()) {
			// skip simId key
			if (key.equals("simId") || key.equals("_id") || key.equals("time")) {
				continue;
			}
			properties.put(key, tState.get(key)
					.toString());
		}
		return Collections.unmodifiableMap(properties);
	}

}
