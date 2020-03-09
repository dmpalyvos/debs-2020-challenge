BEGIN {
  FS=",";
}
{ 
  if ($3 ~ /\./) {
  split($3, a, "\.")
  print $1 "," $2 "," a[1]
  }
  else {
    print $0
  }
}
