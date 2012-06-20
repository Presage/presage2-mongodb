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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import uk.ac.imperial.presage2.core.db.DatabaseService;
import uk.ac.imperial.presage2.core.db.StorageService;
import uk.ac.imperial.presage2.core.db.persistent.PersistentAgent;
import uk.ac.imperial.presage2.core.db.persistent.PersistentSimulation;
import uk.ac.imperial.presage2.core.db.persistent.TransientAgentState;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;

public class MongoStorage implements StorageService, DatabaseService,
		Provider<DB> {

	Mongo mongo;
	DB db;
	final String host;
	final String dbName;
	String username = "";
	String password = "";
	Simulation currentSimulation;

	static class Cache {
		static Map<Long, Simulation> simCache = Collections
				.synchronizedMap(new HashMap<Long, Simulation>());
		static Map<Long, Long> simCacheAge = Collections
				.synchronizedMap(new HashMap<Long, Long>());
		final static long SIM_CACHE_TTL = 5000;
		static Map<UUID, Agent> agentCache = Collections
				.synchronizedMap(new HashMap<UUID, Agent>());

		static void addSimulation(Simulation s) {
			simCache.put(s.getID(), s);
			simCacheAge.put(s.getID(), System.currentTimeMillis());
		}

		static Simulation getSimulation(long id) {
			if (simCache.containsKey(id)
					&& simCacheAge.get(id).longValue() + SIM_CACHE_TTL > System
							.currentTimeMillis()) {
				return simCache.get(id);
			} else {
				return null;
			}
		}

		static void addAgent(Agent a) {
			agentCache.put(a.getID(), a);
		}

		static Agent getAgent(UUID id) {
			if (agentCache.containsKey(id)) {
				return agentCache.get(id);
			} else {
				return null;
			}
		}

		static void clearAgents() {
			agentCache.clear();
		}
	}

	MongoStorage(String host) {
		this(host, "presage");
	}

	@Inject
	MongoStorage(@Named("mongo.host") String host,
			@Named("mongo.db") String dbName) {
		super();
		this.host = host;
		this.dbName = dbName;
	}

	@Inject(optional = true)
	void setUsername(@Named("mongo.user") String username) {
		this.username = username;
	}

	@Inject(optional = true)
	void setPassword(@Named("mongo.pass") String password) {
		this.password = password;
	}

	@Override
	public boolean isStarted() {
		return this.mongo != null && this.db != null;
	}

	@Override
	public void start() throws Exception {
		if (!isStarted()) {
			this.mongo = new Mongo(this.host);
			this.db = this.mongo.getDB(dbName);
			if (this.username != "" && this.password != "") {
				this.db.authenticate(username, password.toCharArray());
			}
		}
	}

	@Override
	public void stop() {
		this.mongo.close();
	}

	@Override
	public synchronized PersistentSimulation createSimulation(String name,
			String classname, String state, int finishTime) {
		this.currentSimulation = new Simulation(name, classname, state,
				finishTime, db);
		Cache.addSimulation(currentSimulation);
		return this.currentSimulation;
	}

	@Override
	public PersistentSimulation getSimulation() {
		return this.currentSimulation;
	}

	@Override
	public PersistentSimulation getSimulationById(long id) {
		Simulation s = Cache.getSimulation(id);
		if (s != null) {
			return s;
		}
		try {
			s = new Simulation(id, db);
			Cache.addSimulation(s);
			return s;
		} catch (NullPointerException e) {
			return null;
		}
	}

	@Override
	public List<Long> getSimulations() {
		DBCursor cursor = db.getCollection(Simulation.simCollection).find(
				new BasicDBObject(), new BasicDBObject());
		List<Long> sims = new LinkedList<Long>();
		while (cursor.hasNext()) {
			sims.add((Long) cursor.next().get("_id"));
		}
		return sims;
	}

	@Override
	public void setSimulation(PersistentSimulation sim) {
		Cache.clearAgents();
		this.currentSimulation = (Simulation) sim;
	}

	@Override
	public PersistentAgent createAgent(UUID id, String name) {
		Agent a = new Agent(id, name, this.currentSimulation, db);
		Cache.addAgent(a);
		this.currentSimulation.addAgent(a);
		return a;
	}

	@Override
	public PersistentAgent getAgent(UUID id) {
		Agent a = Cache.getAgent(id);
		if (a != null) {
			return a;
		}
		try {
			a = new Agent(id, this.currentSimulation, db);
			Cache.addAgent(a);
			return a;
		} catch (NullPointerException e) {
			return null;
		}
	}

	@Override
	public TransientAgentState getAgentState(UUID aid, int time) {
		return getAgent(aid).getState(time);
	}

	@Override
	public DB get() {
		if (!isStarted())
			try {
				start();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		return this.db;
	}

}
