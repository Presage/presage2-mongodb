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

import static org.junit.Assert.*;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.imperial.presage2.core.db.GenericStorageServiceTest;
import uk.ac.imperial.presage2.core.db.persistent.PersistentSimulation;
import uk.ac.imperial.presage2.core.util.random.Random;

public class MongoStorageTest extends GenericStorageServiceTest {

	MongoStorage mongo;

	@Override
	public void getDatabase() {
		mongo = new MongoStorage("localhost", "presage-test");
		// clear db
		try {
			mongo.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
		mongo.db.dropDatabase();
		mongo.db = mongo.mongo.getDB(mongo.dbName);

		this.db = mongo;
		this.sto = mongo;
	}

	@Test
	@Ignore
	public void testCacheExpiry() {
		final String simName = RandomStringUtils.randomAlphanumeric(Random
				.randomInt(20));
		final String simClass = RandomStringUtils.randomAlphanumeric(Random
				.randomInt(100));
		final String simState = RandomStringUtils.randomAlphanumeric(Random
				.randomInt(80));
		final int simFinish = Random.randomInt(100);

		final PersistentSimulation simOrig = sto.createSimulation(simName,
				simClass, simState, simFinish);
		final PersistentSimulation sim1 = sto
				.getSimulationById(simOrig.getID());
		assertSame(sim1, simOrig);

		// wait for expected cache expiry
		try {
			Thread.sleep(MongoStorage.SIM_CACHE_TTL + 5);
		} catch (InterruptedException e) {
		}

		// new object should be returned
		final PersistentSimulation sim2 = sto
				.getSimulationById(simOrig.getID());
		assertNotSame(sim2, simOrig);
	}

}
