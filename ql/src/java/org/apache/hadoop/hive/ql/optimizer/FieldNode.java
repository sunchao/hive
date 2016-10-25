/**
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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FieldNode {
  private final String fieldName;
  private List<FieldNode> nodes;

  public FieldNode(String fieldName) {
    this.fieldName = fieldName;
    nodes = new ArrayList<>();
  }

  public String getFieldName() {
    return fieldName;
  }

  public void addFieldNodes(FieldNode... nodes) {
    if (nodes != null) {
      addFieldNodes(Arrays.asList(nodes));
    }
  }

  public void addFieldNodes(List<FieldNode> nodes) {
    for (FieldNode fn : nodes) {
      if (fn != null) {
        this.nodes.add(fn);
      }
    }
  }

  public List<FieldNode> getNodes() {
    return nodes;
  }

  public void setNodes(List<FieldNode> nodes) {
    this.nodes = nodes;
  }

  public List<String> toPaths() {
    List<String> result = new ArrayList<>();
    if (nodes.isEmpty()) {
      result.add(fieldName);
    } else {
      for (FieldNode child : nodes) {
        for (String rest : child.toPaths()) {
          result.add(fieldName + "." + rest);
        }
      }
    }
    return result;
  }

  public static FieldNode fromString(String path) {
    String[] parts = path.split("\\.");
    return fromString(parts, 0);
  }

  private static FieldNode fromString(String[] parts, int index) {
    if (index == parts.length) {
      return null;
    }
    FieldNode fn = new FieldNode(parts[index]);
    fn.addFieldNodes(fromString(parts, index + 1));
    return fn;
  }

  @Override
  public String toString() {
    String res = fieldName;
    if (nodes.size() > 0) {
      res += "[";
      for (int i = 0; i < nodes.size(); i++) {
        if (i == nodes.size() - 1) {
          res += nodes.get(i).toString();
        } else {
          res += nodes.get(i).toString() + ",";
        }
      }
      res += "]";
    }
    return res;
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof FieldNode)) {
      return false;
    }
    FieldNode fieldNode = (FieldNode) object;
    if (!fieldName.equals(fieldNode.getFieldName()) || fieldNode.getNodes().size() != fieldNode
      .getNodes().size()) {
      return false;
    }

    for (int i = 0; i < fieldNode.getNodes().size(); i++) {
      if (!fieldNode.getNodes().get(i).equals(nodes.get(i))) {
        return false;
      }
    }
    return true;
  }
}
