/*
 * Copyright 2013-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.aggregation;

import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $unwind}-operation.
 * <p>
 * We recommend to use the static factory method {@link Aggregation#unwind(String)} instead of creating instances of
 * this class directly.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Alessio Fachechi
 * @see http://docs.mongodb.org/manual/reference/aggregation/unwind/#pipe._S_unwind
 * @since 1.3
 */
public class UnwindOperation implements AggregationOperation {

	private final ExposedField field;
	private ExposedField includeArrayIndex;
	private boolean preserveNullAndEmptyArrays = false;

	/**
	 * Creates a new {@link UnwindOperation} for the given {@link Field}.
	 *
	 * @param field must not be {@literal null}.
	 */
	public UnwindOperation(Field field) {

		Assert.notNull(field);

		this.field = new ExposedField(field, true);
	}

	// TODO javadoc
	public UnwindOperation(Field field,boolean preserveNullAndEmptyArrays) {

		this(field);
		this.preserveNullAndEmptyArrays = preserveNullAndEmptyArrays;
	}

	// TODO javadoc
	public UnwindOperation(Field field, Field includeArrayIndex, boolean preserveNullAndEmptyArrays) {

		this(field);
		this.includeArrayIndex = new ExposedField(includeArrayIndex, true);
		this.preserveNullAndEmptyArrays = preserveNullAndEmptyArrays;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public DBObject toDBObject(AggregationOperationContext context) {
		BasicDBObject unwindObject = new BasicDBObject();

		unwindObject.append("path", context.getReference(field).toString());
		if (includeArrayIndex != null) {
			unwindObject.append("includeArrayIndex", includeArrayIndex.getTarget());
		}
		unwindObject.append("preserveNullAndEmptyArrays", preserveNullAndEmptyArrays);

		return new BasicDBObject("$unwind", unwindObject);
	}
}
