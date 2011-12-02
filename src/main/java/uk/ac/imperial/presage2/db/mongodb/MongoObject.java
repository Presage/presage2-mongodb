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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class MongoObject extends BasicDBObject {

	private static final long serialVersionUID = 1L;

	public MongoObject() {
		super();
	}

	public MongoObject(DBObject o) {
		super(o.toMap());
	}

	public int getInt(String key) {
		try {
			return Integer.parseInt(this.get(key).toString());
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public void putInt(String key, int value) {
		this.put(key, value);
	}

	public long getLong(String key) {
		try {
			return Long.parseLong(this.get(key).toString());
		} catch (NullPointerException e) {
			return 0L;
		}
	}

	public void putLong(String key, long value) {
		this.put(key, value);
	}

	public String getString(String key) {
		try {
			return this.get(key).toString();
		} catch (NullPointerException e) {
			return null;
		}
	}

	public void putString(String key, String value) {
		this.put(key, value);
	}

}
