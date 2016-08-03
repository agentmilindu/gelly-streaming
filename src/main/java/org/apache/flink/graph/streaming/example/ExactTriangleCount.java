/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.streaming.example;

import com.typesafe.config.ConfigException;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.EventType;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * Single-pass, insertion-only exact Triangle Local and Global Count algorithm.
 * <p>
 * Based on http://www.kdd.org/kdd2016/papers/files/rfp0465-de-stefaniA.pdf.
 */
public class ExactTriangleCount {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SimpleEdgeStream<Integer, NullValue> edges = getGraphStream(env);

		edges.undirected().getEdges().keyBy(0).flatMap(
				// TODO: add some kind of unique timestamp on each edge to be the key?
				new FlatMapFunction<Edge<Integer, NullValue>, Tuple3<Integer, Integer, TreeSet<Integer>>>() {
					Map<Integer, TreeSet<Integer>> neighborhoods = new HashMap<>();
					public void flatMap(Edge<Integer, NullValue> e, Collector<Tuple3<Integer, Integer, TreeSet<Integer>>> out) {
						TreeSet<Integer> t;
						if (neighborhoods.containsKey(e.getSource())) {
							t = neighborhoods.get(e.getSource());
						}
						else {
							t = new TreeSet<>();
						}
						t.add(e.getTarget());
						neighborhoods.put(e.getSource(), t);
						int src = Math.min(e.getSource(), e.getTarget());
						int trg = Math.max(e.getSource(), e.getTarget());
						out.collect(new Tuple3(src, trg, t));
					}
				}).keyBy(0, 1).flatMap(
				new FlatMapFunction<Tuple3<Integer, Integer, TreeSet<Integer>>, Tuple1<Integer>>() {
					public void flatMap(Tuple3<Integer, Integer, TreeSet<Integer>> t, Collector<Tuple1<Integer>> collector) {
						//TODO: intersect neighborhoods and emit local and global counters
			}
		})	//TODO: sum up local and global counters
		;

		env.execute("Exact Triangle Count");
	}

	// *** Transformation Methods *** //


	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String resultPath = null;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length != 2) {
				System.err.println("Usage: ExactTriangleCount <input edges path> <result path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			resultPath = args[1];
		} else {
			System.out.println("Executing ExactTriangleCount example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: ExactTriangleCount <input edges path> <result path>");
		}
		return true;
	}


	@SuppressWarnings("serial")
	private static SimpleEdgeStream<Integer, NullValue> getGraphStream(StreamExecutionEnvironment env) {

		if (fileOutput) {
			return new SimpleEdgeStream<>(env.readTextFile(edgeInputPath)
					.map(new MapFunction<String, Edge<Integer, NullValue>>() {
						@Override
						public Edge<Integer, NullValue> map(String s) {
							String[] fields = s.split("\\s");
							int src = Integer.parseInt(fields[0]);
							int trg = Integer.parseInt(fields[1]);
							return new Edge<>(src, trg, NullValue.getInstance());
						}
					}), env);
		}

		return new SimpleEdgeStream<>(env.fromElements(
				new Edge<>(1, 2, NullValue.getInstance()),
				new Edge<>(2, 3, NullValue.getInstance()),
				new Edge<>(1, 4, NullValue.getInstance()),
				new Edge<>(5, 3, NullValue.getInstance()),
				new Edge<>(3, 4, NullValue.getInstance()),
				new Edge<>(1, 5, NullValue.getInstance())), env);
	}
}
