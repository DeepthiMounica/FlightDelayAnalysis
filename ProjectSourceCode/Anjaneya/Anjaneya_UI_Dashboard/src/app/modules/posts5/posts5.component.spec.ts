import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Posts5Component } from './posts5.component';

describe('Posts5Component', () => {
  let component: Posts5Component;
  let fixture: ComponentFixture<Posts5Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Posts5Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Posts5Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
