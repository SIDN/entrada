#!/usr/bin/env bash

#    This program is made to install all prerquisites to move files from staging
#
#    Copyright (C) 2019 Internetstiftelsen [https:/internetstiftelsen.se/en]
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.

# While the other installers are built to add onto the more basic
# entrada-aws-installer.sh this one is completely standalone.

echo "[$(date)] : Starting Entrada AWS - Staging to DWH - installation"

config=$1
