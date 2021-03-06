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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.presage2.core.db.persistent.PersistentAgent;
import uk.ac.imperial.presage2.core.db.persistent.PersistentEnvironment;
import uk.ac.imperial.presage2.core.db.persistent.PersistentSimulation;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class Simulation implements PersistentSimulation {

	final static String simCollection = "simulations";
	final DB db;
	final DBCollection sims;
	final MongoObject object;
	final Environment env;

	Simulation(String name, String classname, String state, int finishTime,
			DB db) {
		super();
		this.db = db;
		sims = db.getCollection(simCollection);
		object = new MongoObject();
		DBObject simId = db.getCollection("counters").findAndModify(
				new BasicDBObject("_id", simCollection), null, null, false,
				new BasicDBObject("$inc", new BasicDBObject("next", 1L)), true,
				true);
		object.putLong("_id", (Long) simId.get("next"));
		object.putString("name", name);
		object.putString("classname", classname);
		object.putString("state", state);
		object.putInt("finishTime", finishTime);
		object.putLong("createdAt", Long.valueOf(new Date().getTime()));
		object.putInt("currentTime", 0);
		object.putLong("finishedAt", 0L);
		object.put("parameters", new MongoObject());
		object.putLong("parent", 0L);
		object.put("children", new BasicDBList());
		sims.insert(object);
		env = new Environment(getID(), db);
	}

	Simulation(long id, DB db) {
		super();
		this.db = db;
		sims = db.getCollection(simCollection);
		object = new MongoObject(sims.findOne(new BasicDBObject("_id", id)));
		env = new Environment(getID(), db);
	}

	@Override
	public String getClassName() {
		return object.getString("classname");
	}

	@Override
	public long getCreatedAt() {
		return object.getLong("createdAt");
	}

	@Override
	public int getCurrentTime() {
		return object.getInt("currentTime");
	}

	@Override
	public int getFinishTime() {
		return object.getInt("finishTime");
	}

	@Override
	public long getFinishedAt() {
		return object.getLong("finishedAt");
	}

	@Override
	public long getID() {
		return object.getLong("_id");
	}

	@Override
	public String getName() {
		return object.getString("name");
	}

	@Override
	public PersistentSimulation getParentSimulation() {
		long parent = object.getLong("parent");
		if (parent > 0L) {
			Simulation parentSimulation = MongoStorage.Cache
					.getSimulation(parent);
			if (parentSimulation == null) {
				parentSimulation = new Simulation(parent, db);
				MongoStorage.Cache.addSimulation(parentSimulation);
			}
			return parentSimulation;
		} else
			return null;
	}

	@Override
	public void setParentSimulation(PersistentSimulation parent) {
		// remove child from previous parent
		PersistentSimulation previousParent = getParentSimulation();
		if (previousParent != null) {
			((Simulation) previousParent).removeChild(this);
		}
		long parentId;
		if (parent == null) {
			parentId = 0;
		} else {
			parentId = parent.getID();
			((Simulation) parent).addChild(this);
		}
		object.putLong("parent", parentId);
		sims.save(object);
	}

	@Override
	public long getStartedAt() {
		return object.getLong("startedAt");
	}

	@Override
	public String getState() {
		return object.getString("state");
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, String> getParameters() {
		DBObject params = (DBObject) object.get("parameters");
		return params.toMap();
	}

	@Override
	public void addParameter(String key, String value) {
		DBObject params = (DBObject) object.get("parameters");
		params.put(key, value);
		sims.save(object);
	}

	@Override
	public Set<PersistentAgent> getAgents() {
		Set<PersistentAgent> agents = new HashSet<PersistentAgent>();
		DBCollection agentsCollection = db.getCollection(Agent.agentCollection);
		// query agents collection for this sim id
		for (DBObject agent : agentsCollection.find(new BasicDBObject("simID",
				getID()))) {
			agents.add(new Agent(agent, db));
		}
		return agents;
	}

	public void addAgent(PersistentAgent a) {
		// we don't add agents to the simulation object - use weak reference
		// from agents collection
	}

	@Override
	public void setCurrentTime(int time) {
		object.putInt("currentTime", time);
		sims.save(object);
	}

	@Override
	public void setFinishedAt(long time) {
		object.putLong("finishedAt", time);
		sims.save(object);
	}

	@Override
	public void setStartedAt(long time) {
		object.putLong("startedAt", time);
		sims.save(object);
	}

	@Override
	public void setState(String state) {
		object.putString("state", state);
		sims.save(object);
	}

	@Override
	public PersistentEnvironment getEnvironment() {
		return env;
	}

	public void addChild(PersistentSimulation sim) {
		BasicDBList children = (BasicDBList) object.get("children");
		children.add(sim.getID());
		sims.save(object);
	}

	public void removeChild(PersistentSimulation sim) {
		BasicDBList children = (BasicDBList) object.get("children");
		children.remove(sim.getID());
		sims.save(object);
	}

	@Override
	public List<Long> getChildren() {
		BasicDBList list = (BasicDBList) object.get("children");
		List<Long> children = new ArrayList<Long>(list.size());
		for (Object child : list) {
			children.add(Long.parseLong(child.toString()));
		}
		return children;
	}

}
