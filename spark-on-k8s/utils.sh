#!/usr/bin/env bash

function error {
  echo "$@" 1>&2
  exit 1
}

function dir_exists {
  [ -d $"$@" ]
}

function file_exists {
  [ -f $"$@" ]
}

function dir_exists_or_err {
  local dir="$@"
  if dir_exists $dir; then
    echo "$dir exists."
    return 0
  else
    error "$dir does not exists!"
  fi
}

function file_exists_or_err {
  local f="$@"
  if file_exists $f; then
    echo "$f exists."
    return 0
  else
    error "$f does not exists!"
  fi
}

function command_exists {
  command -v "$@" >/dev/null 2>&1
}

function command_exists_or_err {
  local cmd="$@"
  if command_exists "$cmd"; then
    echo "The command '${cmd}' exists."
  else
   error "The command '${cmd}' do not exist!"
  fi
}