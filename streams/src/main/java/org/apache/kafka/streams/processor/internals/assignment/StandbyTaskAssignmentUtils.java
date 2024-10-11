/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

final class StandbyTaskAssignmentUtils {
    private StandbyTaskAssignmentUtils() {}

    static ConstrainedPrioritySet createLeastLoadedPrioritySetConstrainedByAssignedTask(final Map<UUID, ClientState> clients) {
        return new ConstrainedPrioritySet((client, t) -> !clients.get(client).hasAssignedTask(t),
                                          client -> clients.get(client).assignedTaskLoad());
    }

    static void pollClientAndMaybeAssignAndUpdateRemainingStandbyTasks(final int numStandbyReplicas,
                                                                       final Map<UUID, ClientState> clients,
                                                                       final Map<TaskId, Integer> tasksToRemainingStandbys,
                                                                       final ConstrainedPrioritySet standbyTaskClientsByTaskLoad,
                                                                       final TaskId activeTaskId,
                                                                       final Logger log) {
        int numRemainingStandbys = tasksToRemainingStandbys.get(activeTaskId);
        while (numRemainingStandbys > 0) {
            final UUID client = standbyTaskClientsByTaskLoad.poll(activeTaskId);
            if (client == null) {
                break;
            }
            clients.get(client).assignStandby(activeTaskId);
            numRemainingStandbys--;
            standbyTaskClientsByTaskLoad.offer(client);
            tasksToRemainingStandbys.put(activeTaskId, numRemainingStandbys);
        }

        if (numRemainingStandbys > 0) {
            log.warn("Unable to assign {} of {} standby tasks for task [{}]. " +
                     "There is not enough available capacity. You should " +
                     "increase the number of application instances " +
                     "to maintain the requested number of standby replicas.",
                     numRemainingStandbys, numStandbyReplicas, activeTaskId);
        }
    }

    static Map<TaskId, Integer> computeTasksToRemainingStandbys(final int numStandbyReplicas,
                                                                final Set<TaskId> statefulTaskIds) {
        return statefulTaskIds.stream().collect(toMap(Function.identity(), t -> numStandbyReplicas));
    }
}
