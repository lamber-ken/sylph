/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ideal.common.graph.impl;

import ideal.common.graph.Node;

public class DemoNode
        extends Node<Void>
{
    private final String id;
    public DemoNode(String id)
    {
        this.id = id;
    }

    @Override
    public String getId()
    {
        return id;
    }

    @Override
    public String getName()
    {
        return getId();
    }

    @Override
    public Void getOutput()
    {
        return null;
    }

    @Override
    public void action(Node<Void> parentNode)
    {
        if (parentNode == null) { //根节点
            System.out.println("我是: 根节点" + toString());
        }
        else {  //叶子节点
            System.out.println("我是:" + toString() + "来自:" + parentNode.toString() + "-->" + toString());
        }
    }

    @Override
    public String toString()
    {
        return "node:" + getId();
    }
}
