*     pgload - routines for connecting Postgres databases to Stata
*     Copyright (C) 2007 Andrew Chadwick
* 
*     This program is free software: you can redistribute it and/or modify it
*     under the terms of the GNU Lesser General Public License as published by
*     the Free Software Foundation, either version 3 of the License, or (at your
*     option) any later version.
* 
*     This program is distributed in the hope that it will be useful,
*     but WITHOUT ANY WARRANTY; without even the implied warranty of
*     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*     GNU Lesser General Public License for more details.
*     
*     You should have received a copy of the GNU General Public License and the
*     GNU Lesser General Public License along with this program.  If not, see
*     <http://www.gnu.org/licenses/>.


program define pgload
    version 9.2
    args conninfo sqlquery
    syntax [anything] [, debug clear]

    if ("`clear'" == "clear") {
        capture clear
        if (_rc!=0) {
            display as error "Dataset clear failed."
            exit _rc
        }
    }

    if "`conninfo'"==""|"`sqlquery'"=="" {
        display as error "usage: pgload CONNECTSTRING SQLQUERY"
        exit 198
    }

    * Connect
    capture noisily plugin call pg, connect "`conninfo'" "`debug'"
    if (_rc!=0) {
        exit _rc
    }

    * Prepare query cursor
    capture noisily plugin call pg, prepare "`sqlquery'" "`debug'"
    if (_rc!=0) {
        display as error "Database prepare statements failed."
        plugin call pg, disconnect "`debug'"
        exit _rc
    }

    * Debug info, show columns and types
    if ("`debug'" == "debug") {
        display "---------------------------"
        display "Columns: `vars'"
        display "Types: `types'"
        display "Formats: `fmts'"
        display "---------------------------"
    }

    * Set types
    capture noisily {
        local stop : word count `vars'
        forvalues i = 1/`stop' {
            local var : word `i' of `vars'
            local type : word `i' of `types'
            local fmt : word `i' of `fmts'
            if ("`debug'" == "debug") {
                display " `var' is `type', format: `fmt'"
            }
            if strpos("`type'", "str") > 0 {
                qui gen `type' `var' = ""
            }
            else {
                qui gen `type' `var' = .
            }
            if (strpos("`fmt'", "default") == 0) {
                format `var' `fmt'
            }
        }
    }
    if (_rc!=0) {
        display as error "Couldn't set up Stata types and formats"
        plugin call pg, disconnect "`debug'"
        exit _rc
    }

    * Repeatedly grow the workspace and load the next batch of data.
    while _rc==0 {
        capture set obs `obs'
        if (_rc!=0) {
            display as error "Failed to grow workspace."
            display as error "As a possible solution, try increasing memory using -set memory-"
            * Force dataset clear to avoid partially-loaded datasets
            clear
            plugin call pg, disconnect "`debug'"
            exit _rc
        }
        capture noisily plugin call pg, populate_next "`debug'"
        if (_rc!=0 & _rc!=1) {
            display as error "Expected either 0 or 1 from populate"
            plugin call pg, disconnect "`debug'"
            exit _rc
        }
    }

    * And finish.
    plugin call pg, disconnect "`debug'"
end

program pg, plugin
