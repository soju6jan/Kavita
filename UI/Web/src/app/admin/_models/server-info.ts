export interface ServerInfoSlim {
  kavitaVersion: string;
  modVersion: string;
  installId: string;
  isDocker: boolean;
  firstInstallVersion?: string;
  firstInstallDate?: string;
}
