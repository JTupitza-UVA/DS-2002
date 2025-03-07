/*
 * Copyright (c) 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.otel;

import com.mysql.cj.telemetry.TelemetryAttribute;
import com.mysql.cj.telemetry.TelemetryScope;
import com.mysql.cj.telemetry.TelemetrySpan;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;

public class OpenTelemetrySpan implements TelemetrySpan {

    private Span span = null;
    private OpenTelemetryScope scope = null;

    public OpenTelemetrySpan(Span span) {
        this.span = span;
    }

    @Override
    public TelemetryScope makeCurrent() {
        this.scope = new OpenTelemetryScope(this.span.makeCurrent());
        return this.scope;
    }

    @Override
    public void setAttribute(TelemetryAttribute key, String value) {
        this.span.setAttribute(key.getKey(), value);
    }

    @Override
    public void setAttribute(TelemetryAttribute key, long value) {
        this.span.setAttribute(key.getKey(), value);
    }

    @Override
    public void setError(Throwable cause) {
        this.span.setStatus(StatusCode.ERROR, cause.getMessage()).recordException(cause);
    }

    @Override
    public void end() {
        this.span.end();
    }

    @Override
    public void close() {
        if (this.scope != null) {
            this.scope.close();
            this.scope = null;
        }
        end();
    }

}
