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
import uk.ac.imperial.presage2.core.db.Transaction;
import uk.ac.imperial.presage2.core.db.persistent.PersistentAgent;
import uk.ac.imperial.presage2.core.db.persistent.PersistentSimulation;
import uk.ac.imperial.presage2.core.db.persistent.SimulationFactory;
import uk.ac.imperial.presage2.core.db.persistent.TransientAgentState;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;

public class MongoStorage implements StorageService, DatabaseService {

	Mongo mongo;
	DB db;
	final String host;
	String dbName = "presage";
	String username = "";
	String password = "";
	Simulation currentSimulation;

	Map<Long, Simulation> simCache = Collections
			.synchronizedMap(new HashMap<Long, Simulation>());
	Map<UUID, Agent> agentCache = Collections
			.synchronizedMap(new HashMap<UUID, Agent>());

	@Inject
	MongoStorage() {
		super();
		this.host = "localhost";
	}

	@Inject
	MongoStorage(@Named("mongo.host") String host) {
		super();
		this.host = host;
	}

	@Inject
	MongoStorage(@Named("mongo.host") String host,
			@Named("mongo.user") String username,
			@Named("mongo.pass") String password) {
		super();
		this.host = host;
		this.username = username;
		this.password = password;
	}

	MongoStorage(String host, String dbName) {
		super();
		this.host = host;
		this.dbName = dbName;
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
		this.simCache.put(this.currentSimulation.getID(),
				this.currentSimulation);
		this.agentCache.clear();
		return this.currentSimulation;
	}

	@Override
	public PersistentSimulation getSimulation() {
		return this.currentSimulation;
	}

	@Override
	public PersistentSimulation getSimulationById(long id) {
		if (this.simCache.containsKey(id)) {
			return this.simCache.get(id);
		}
		try {
			Simulation s = new Simulation(id, db);
			this.simCache.put(s.getID(), s);
			return s;
		} catch (NullPointerException e) {
			return null;
		}
	}

	@Override
	public SimulationFactory getSimulationFactory() {
		throw new UnsupportedOperationException();
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
		this.agentCache.clear();
		this.currentSimulation = (Simulation) sim;
	}

	@Override
	public PersistentAgent createAgent(UUID id, String name) {
		Agent a = new Agent(id, name, this.currentSimulation, db);
		this.agentCache.put(id, a);
		this.currentSimulation.addAgent(a);
		return a;
	}

	@Override
	public PersistentAgent getAgent(UUID id) {
		if (agentCache.containsKey(id)) {
			return agentCache.get(id);
		}
		try {
			Agent a = new Agent(id, this.currentSimulation, db);
			agentCache.put(id, a);
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
	public Transaction startTransaction() {
		throw new UnsupportedOperationException();
	}

}
