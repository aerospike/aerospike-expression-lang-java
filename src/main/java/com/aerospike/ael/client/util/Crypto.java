/*
 * Copyright 2012-2024 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.ael.client.util;

import gnu.crypto.util.Base64;

public final class Crypto {

    /**
     * Decode base64 bytes into a byte array.
     */
    public static byte[] decodeBase64(byte[] src, int off, int len) {
        return Base64.decode(src, off, len);
    }

    /**
     * Encode bytes into a base64 encoded string.
     */
    public static String encodeBase64(byte[] src) {
        return Base64.encode(src, 0, src.length, false);
    }
}
