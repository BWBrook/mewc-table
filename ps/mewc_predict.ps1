# Run MEWC-classifier-inference on a Service, recursing through all subdirectories that contain image files

param (
  [string]$i = ".\",
  [string]$p = ".\",
  [string]$c = ".\",
  [string]$m = ".\",
  [string]$g = ".\"
)

$SERVICE_DIR = (Resolve-Path -Path $i) | Convert-Path
$PARAM_ENV = (Resolve-Path -Path $p) | Convert-Path
$CLASS = (Resolve-Path -Path $c) | Convert-Path
$MEWC_MODEL = (Resolve-Path -Path $m) | Convert-Path

Function MEWC_SCRIPT {
  Param($IN_DIR, $PARAMS, $CL, $MODEL)
  Write-Host "Site Directory: $IN_DIR"
  $docker_predict = "docker run --env CUDA_VISIBLE_DEVICES=$g --env-file $PARAMS --gpus all --interactive --tty --rm --volume `"${IN_DIR}:/images`" --mount type=bind,source=$MODEL,target=/code/model.keras --mount type=bind,source=$CL,target=/code/class_map.yaml zaandahl/mewc-predict"
  Invoke-Expression $docker_predict
}

docker pull zaandahl/mewc-predict
MEWC_SCRIPT $SERVICE_DIR $PARAM_ENV $CLASS $MEWC_MODEL
