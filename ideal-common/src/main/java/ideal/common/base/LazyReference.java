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
package ideal.common.base;

import java.io.Serializable;

/**
 * Let java like scala use lazy freely
 * google guava {@link com.google.common.base.Suppliers#memoize}
 */
public class LazyReference<T>
        implements Serializable
{
    private static final long serialVersionUID = 0L;
    private final Supplier<T> supplier;
    private transient volatile T instance;

    private LazyReference(Supplier<T> supplier)
    {
        this.supplier = supplier;
    }

    public static <T> LazyReference<T> goLazy(Supplier<T> supplier)
    {
        return new LazyReference<>(supplier);
    }

    public T get()
    {
        if (instance == null) {
            synchronized (supplier) {  //1
                if (instance == null) {          //2
                    instance = supplier.get();  //3
                }
            }
        }
        return instance;
    }

    @FunctionalInterface
    public interface Supplier<T>
            extends Serializable
    {
        T get();
    }
}
